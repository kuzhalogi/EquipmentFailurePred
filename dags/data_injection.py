import os
from utils import load_criticality_config, get_criticality, send_teams_alert 
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

RAW_DATA='/home/kuzhalogi/WorkSpace/EquipmentFailurePred/raw-data'
SUITE_NAME = "milling_machine_data_quality"
GOOD_DATA_FOLDER = "/home/kuzhalogi/WorkSpace/Equipmentfailurepred/data/good_data"
BAD_DATA_FOLDER = "/home/kuzhalogi/WorkSpace/Equipmentfailurepred/data/bad_data"
DATABASE_CONN_STR = 'postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres'
GREAT_EXPECTATION = '/home/kuzhalogi/WorkSpace/EquipmentFailurePred/gx'

criticality_config = load_criticality_config()

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
        context = gx.data_context.DataContext(GREAT_EXPECTATION)
        suite = context.get_expectation_suite(SUITE_NAME)
        df = gx.read_csv(file_path)
        
        results = df.validate(expectation_suite=suite)
        capsule = {'results': results, 'file_path': file_path, 'errors': [],'stats':[]}
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
        error_rate = round((invalid_rows / total_rows) * 100,2)
        processed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Save statistics into the database
        engine = sa.create_engine(DATABASE_CONN_STR)
        connection = engine.connect()
        metadata = sa.MetaData()
        
        # Reflect the data_validation_summary table from the database
        stats_table = sa.Table('data_validation_summary', metadata, autoload_with=engine)

        stats_data = {
            "file_name": file_name,
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "error_rate": error_rate,
            "processed_at": processed_at  
        }

        connection.execute(stats_table.insert().values(stats_data))
        connection.close()
        capsule['stats'].append(stats_data)

    @task
    def send_alert(capsule):
        validation_result = capsule["results"]
    
        if validation_result["success"]:
            logging.info("Validation successful, no alert sent.")
            return
        
        # Create a summary of the validation failures
        alert_message = "Data validation failed. Issues detected:\n"
        
         # Loop through the results to build the alert message based on criticality
        for result in validation_result.get("results", []):
            expectation_config = result.get("expectation_config", {})
            expectation_type = expectation_config.get("expectation_type", "Unknown")
            
            # Safely access 'column' key
            column = expectation_config.get("kwargs", {}).get("column", "Unknown")
            criticality = get_criticality(expectation_type, column, criticality_config)  # Get criticality based on the column and expectation type
            
            if not result.get("success", True):  # Check if validation failed
                alert_message += f"\n- Column: {column}, Expectation: {expectation_type}, Criticality: {criticality}, Issues: {result.get('result', 'No issues reported')}"
        
        # If there were any validation issues, send the alert
        if alert_message.strip() != "Data validation failed. Issues detected:":
            send_teams_alert(alert_message)  # Send the alert (e.g., via Microsoft Teams)

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

