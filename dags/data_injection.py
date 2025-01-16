import os
from utils import get_criticality, send_teams_alert 
import random
import logging
import pandas as pd
import great_expectations as gx
import sqlalchemy as sa
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import BranchPythonOperator
from airflow.exceptions import AirflowSkipException

RAW_DATA='/home/kuzhalogi/WorkSpace/Equipmenfailurepred/raw-data'
SUITE_NAME = "milling_machine_data_quality"
GOOD_DATA_FOLDER = "/home/kuzhalogi/WorkSpace/Equipmenfailurepred/data/good_data"
BAD_DATA_FOLDER = "/home/kuzhalogi/WorkSpace/Equipmenfailurepred/data/bad_data"
DATABASE_CONN_STR = 'postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/data_errors'


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
        files = os.listdir(raw_data_folder) #listing all files
        selected_file = random.choice(files) # selecting randomly
        
        # getting file number from the file name
        file_number = os.path.splitext(os.path.basename(selected_file))[0].split('_')[-1] 
        
        # checking the file is validated or not
        if file_number not in validated:
            to_read = os.path.join(raw_data_folder, selected_file) # this file will be approved
            file_number = os.path.splitext(os.path.basename(to_read))[0].split('_')[-1]
            validated.append(file_number)
        else: # else an expection will rise, say it already validated. and the dag will be skipped
            logging.info(f"Procced file number {file_number} detected, skipping the DAG.")
            raise AirflowSkipException("Proceed file detected, skipping the DAG.")
        return to_read

    @task
    def validate_data(file_path: str):
        context = gx.data_context.DataContext()
        suite = context.get_expectation_suite(SUITE_NAME)
        df = gx.read_csv(file_path)
        results = df.validate(expectation_suite=suite)
        capsule = {'results': results, 'file_path': file_path, 'errors': []}
        if not results["success"]:
            for result in results["results"]:
                if not result["success"]:
                    error = {
                        "expectation": result["expectation_config"],
                        "unexpected_value": result["result"].get("unexpected_list", [])
                    }
                    capsule['errors'].append(error)
        return capsule

    @task
    def save_statistics(capsule):
        errors = capsule["errors"]
        total_rows = len(pd.read_csv(capsule["file_path"]))
        valid_rows = total_rows - len(errors)
        invalid_rows = len(errors)
        file_name = os.path.basename(capsule["file_path"])

        # Save statistics into the database
        engine = sa.create_engine(DATABASE_CONN_STR)
        connection = engine.connect()
        metadata = sa.MetaData()
        stats_table = sa.Table('data_statistics', metadata, autoload_with=engine)
        stats_data = {
            "file_name": file_name,
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "error_details": str(errors)  # Or serialize the error info into a JSON blob
        }
        connection.execute(stats_table.insert(), stats_data)
        connection.close()


    @task
    def send_alert(capsule):
        validation_result = capsule["results"]
    
        if validation_result["success"]:
            logging.info("Validation successful, no alert sent.")
            return
        
        # Create a summary of the validation failures
        alert_message = "Data validation failed. Issues detected:\n"
        
        # Loop through the results to build the alert message based on criticality
        for result in validation_result["results"]:
            expectation_type = result["expectation_config"]["expectation_type"]
            column = result["expectation_config"]["kwargs"]["column"]
            criticality = get_criticality(expectation_type, column)  # Get criticality based on the column and expectation type
            
            if not result["success"]:
                alert_message += f"\n- Column: {column}, Expectation: {expectation_type}, Criticality: {criticality}, Issues: {result['result']}"
        
        # Here, you can use your alerting system to send the message (e.g., Microsoft Teams notification)
        send_teams_alert(alert_message)

        # Log the message for debugging purposes
        logging.info(f"Alert sent: {alert_message}")
        

    @task
    def split_and_save_files(capsule,error_info):
        pass





    t1 = read_data(RAW_DATA)
    t2 = validate_data(t1)
    t3 = save_statistics(t2)
    t4 = send_alert(t2)
    t5 = split_and_save_files(t2,error_info)

    t1 >> t2 >> [t3, t4, t5]

first_dag = data_injection()

