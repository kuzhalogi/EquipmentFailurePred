import os 
import random
import shutil
import logging
import pandas as pd
import great_expectations as gx
import sqlalchemy as sa
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import BranchPythonOperator
from airflow.exceptions import AirflowSkipException

RAW_DATA='/home/kuzhalogi/dsp_project/raw-data'
SUITE_NAME = "expect_onlyfeatures"
GOOD_DATA_FOLDER = "/home/kuzhalogi/dsp_project/data/good_data"
BAD_DATA_FOLDER = "/home/kuzhalogi/dsp_project/data/bad_data"
DATABASE_CONN_STR = 'postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/data_errors'

high_criticality_expectations = [
    "expect_column_values_to_not_be_null:Product ID",
    "expect_column_values_to_not_be_null:Type",
    "expect_column_values_to_not_be_null:Air temperature [K]",
    "expect_column_values_to_not_be_null:Process temperature [K]",
    "expect_column_values_to_not_be_null:Rotational speed [rpm]",
    "expect_column_values_to_not_be_null:Torque [Nm]",
    "expect_column_values_to_not_be_null:Tool wear [min]",
    "expect_column_values_to_be_between:Process temperature [K]",
    "expect_column_values_to_be_between:Air temperature [K]",
    "expect_column_values_to_be_between:Rotational speed [rpm]",
    "expect_column_values_to_be_between:Torque [Nm]",
    "expect_column_values_to_be_between:Tool wear [min]"
]

medium_criticality_expectations = [
    "expect_column_values_to_be_of_type:Air temperature [K]",
    "expect_column_values_to_be_of_type:Process temperature [K]",
    "expect_column_values_to_be_of_type:Rotational speed [rpm]",
    "expect_column_values_to_be_of_type:Torque [Nm]",
    "expect_column_values_to_be_of_type:Tool wear [min]",
    "expect_column_values_to_be_of_type:Product ID",
    "expect_column_values_to_be_of_type:Type"
    
]

low_criticality_expectations = [
    "expect_column_values_to_match_regex:Product ID"
]

def get_criticality(expectation_type, column_name):
    expectation = f"{expectation_type}:{column_name}"
    if expectation in high_criticality_expectations:
        return "high"
    elif expectation in medium_criticality_expectations:
        return "medium"
    elif expectation in low_criticality_expectations:
        return "low"
    else:
        return "unknown"

validated = []
error_info = []

@dag(
    dag_id='data_injection',
    description=' checks for issues and separate good and bad data from the .csv file ',
    tags=['dsp'],
    schedule=timedelta(minutes=5),
    start_date=days_ago(n=0, hour=1)
)
def data_injection():
    @task
    def read_data(raw_data_folder: str):
        files = os.listdir(raw_data_folder)
        selected_file = random.choice(files)
        file_number = os.path.splitext(os.path.basename(selected_file))[0].split('_')[-1]
        if file_number not in validated:
            to_read = os.path.join(raw_data_folder, selected_file)
            file_number = os.path.splitext(os.path.basename(to_read))[0].split('_')[-1]
            validated.append(file_number)
        else:
            logging.info(f"Procced file number {file_number} detected, skipping the DAG.")
            raise AirflowSkipException("Proceed file detected, skipping the DAG.")
        return to_read

    @task
    def validate_data(file_path: str):
        context = gx.data_context.DataContext()
        suite = context.get_expectation_suite(SUITE_NAME)
        df = gx.read_csv(file_path)
        results = df.validate(expectation_suite=suite)
        if results["success"]:
            if not os.path.exists(GOOD_DATA_FOLDER):
                os.makedirs(GOOD_DATA_FOLDER)
                shutil.move(file_path, os.path.join(GOOD_DATA_FOLDER, os.path.basename(file_path)))
            logging.info(f"This file at {file_path} passed the data validation")
        else:
            capsule = {'results':results,'file_path':file_path}
            return capsule

    @task
    def split_and_save_data(capsule):
        validation_result = capsule["results"]
        file_path = capsule["file_path"]
        if validation_result["success"]:
            return
        if not os.path.exists(GOOD_DATA_FOLDER):
            os.makedirs(GOOD_DATA_FOLDER)
        if not os.path.exists(BAD_DATA_FOLDER):
            os.makedirs(BAD_DATA_FOLDER)

        file_number = os.path.splitext(os.path.basename(file_path))[0].split('_')[-1]

        df = pd.read_csv(file_path)
        
        good_rows = []
        bad_rows = []
        
        for idx, row in df.iterrows():
            row_valid = True
            if row.isnull().any():
                row_valid = False
                missing_columns = row.index[row.isnull()].tolist()
                for column in missing_columns:
                    error_info.append({
                        "file_number": file_number, 
                        "row_index": idx, 
                        "column": column, 
                        "error": "Missing value", 
                        "criticality": "high"
                    })
                    
            for result in validation_result["results"]:
                column = result["expectation_config"]["kwargs"]["column"]
                success = result["success"]
                expectation_type = result["expectation_config"]["expectation_type"]
                criticality = get_criticality(expectation_type, column)

                if not success:
                    unexpected_values = result["result"].get("partial_unexpected_list", [])
                    if column and row[column] in unexpected_values:
                        row_valid = False
                        error_info.append({
                            "file_number": file_number, 
                            "row_index": idx, 
                            "column": column, 
                            "error": row[column], 
                            "criticality": criticality
                        })
            
            if row_valid:
                good_rows.append(row)
            else:
                bad_rows.append(row)

        good_data = pd.DataFrame(good_rows)
        bad_data = pd.DataFrame(bad_rows)

        good_data_filename = f'good_data_{file_number}.csv'
        bad_data_filename = f'bad_data_{file_number}.csv'

        good_data_path = os.path.join(GOOD_DATA_FOLDER, good_data_filename)
        bad_data_path = os.path.join(BAD_DATA_FOLDER, bad_data_filename)

        good_data.to_csv(good_data_path, index=False)
        bad_data.to_csv(bad_data_path, index=False)


    @task
    def send_alert(capsule):
        validation_result = capsule["results"]
        if validation_result["success"]:
            return
        

    @task
    def save_data_errors(capsule,error_info):
        validation_result = capsule["results"]
        file_path = capsule['file_path']
        if validation_result["success"]:
            return
        errors = []
        for result in validation_result["results"]:
            column = result["expectation_config"]["kwargs"]["column"]
            success = result["success"]
            expectation_type = result["expectation_config"]["expectation_type"]
            criticality = get_criticality(expectation_type, column)
            if not success:
                for idx, value in enumerate(result["result"].get("partial_unexpected_list", [])):
                    errors.append({
                        "timestamp": datetime.now(),
                        "file_number": file_path,
                        "row_index": idx,
                        "column": column,
                        "error": value,
                        "criticality": criticality
                    })
        if errors:
            engine = sa.create_engine(DATABASE_CONN_STR)
            connection = engine.connect()
            metadata = sa.MetaData()
            data_errors = sa.Table('data_errors', metadata, autoload_with=engine)
            
            for error_info in errors:
                connection.execute(data_errors.insert(), error_info)
            
            connection.close()
        return errors

    @task
    def send_alerts(validation_result):
        if not validation_result["success"]:
            print("Sending alert: Data validation failed.")
        else:
            print("Validation successful, no alert sent.")




    t1 = read_data(RAW_DATA)
    t2 = validate_data(t1)
    t3 = split_and_save_data(t2)
    t4 = send_alert(t2)
    t5 = save_data_errors(t2,error_info)

    t1 >> t2 >> [t3, t4, t5]

first_dag = data_injection()

