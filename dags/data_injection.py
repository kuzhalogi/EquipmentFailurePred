import os
import random
from dag_utils import *
from dbhelper import *
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import BranchPythonOperator
from airflow.exceptions import AirflowSkipException

processed_files = set()

@dag(
    dag_id='data_injection',
    description=' checks for issues and separate good and bad data from the .csv file ',
    tags=['dsp'],
    schedule=timedelta(minutes=5),
    start_date=days_ago(n=0, hour=1)
)


def data_injection():
     # hold the validated file numbers, used to avoid revalidating the same file. 
    @task #read new data form raw_data folder
    def read_data(raw_data_folder: str):
        # List all files in the raw data folder
        files = [f for f in os.listdir(raw_data_folder) if os.path.isfile(os.path.join(raw_data_folder, f))]
        
        # If no files are available, skip the DAG
        if not files:
            logging.info("No files found in the raw data folder, skipping the DAG.")
            raise AirflowSkipException("No files found in the raw data folder, skipping the DAG.")
        
        # Select a random file
        selected_file = random.choice(files)
        full_file_path = os.path.join(raw_data_folder, selected_file)
        
        # Check if the file has already been validated
        if selected_file in processed_files:
            logging.info(f"File '{selected_file}' has already been validated, skipping the DAG.")
            raise AirflowSkipException(f"File '{selected_file}' has already been validated, skipping the DAG.")
        
        # Mark the file as validated
        processed_files.add(selected_file)
        logging.info(f"Selected file for processing: {selected_file}")
        
        return full_file_path


    @task #validating each file
    def validate_data(file_path: str):
        
        df, suite = load_validation_suite(file_path)
        rows_validation_list = validate_rows(df, suite)
        IsBadData = False if len(rows_validation_list) == 0 else True
        
        if not IsBadData:
            logging.info("The file has good data skipping the next 3 tasks")
        else:
            # Converting those falied validation errors into DataFrame for easy handling
            failed_validations_df = pd.concat(rows_validation_list, ignore_index=True) 
        
        validation_stats = get_validation_stats(df,failed_validations_df)
        
        capsule = {
        'validations_failed_df': failed_validations_df, 
        'file_path': file_path,
        'stats': {
            "file_name": os.path.basename(file_path),
            "total_rows": validation_stats["total_rows"],
            "valid_rows": validation_stats["good_rows"],
            "invalid_rows": validation_stats["bad_rows"],
            "error_rate": round(validation_stats["failure_rate"], 2),
            "processed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }}
        
        return capsule
    

    @task
    def save_statistics(capsule):
        failed_validations_df = capsule["validations_failed_df"]   
        erros_statistics = capsule["stats"]

        total_rows_count = erros_statistics["total_rows"]
        valid_rows_count = erros_statistics["valid_rows"]
        invalid_rows_count = erros_statistics["invalid_rows"]
        file_name = erros_statistics["file_name"]
        error_rate = round(erros_statistics["error_rate"],2)
        
        expectations, columns = get_failed_stats(failed_validations_df)
        
        # Save statistics to the database
        save_validation_statistics(file_name, total_rows_count, valid_rows_count, invalid_rows_count, error_rate)
        save_failed_expectations(file_name, expectations)
        save_failed_columns(file_name, columns)
    
        return capsule


    @task
    def send_alert(capsule):
        # Check if the capsule is None or missing required keys
        if capsule is None or "stats" not in capsule:
            logging.error("Capsule is incomplete. No alert sent.")
            return
        
        # Extract the error rate from the stats
        error_rate = capsule["stats"]["error_rate"]
        if error_rate is None:
            logging.error("Error rate not found in the stats. No alert sent.")
            return
        criticality = get_criticality(error_rate)
        
        alert_message = build_alert_message(capsule, criticality)
    
        # Send the alert if there are issues
        if alert_message.strip() != "Data validation failed. Issues detected:":
            send_teams_alert(alert_message)
            logging.info(f"Alert sent: {alert_message}")


    @task
    def split_and_save_files(capsule):
        """
        Split the DataFrame into good and bad rows, and save them to respective directories.
        """
        file_name = capsule["stats"]["file_name"]
        file_path = capsule["file_path"]
        failed_validations_df = capsule["validations_failed_df"]

        # Load Data
        df = load_dataframe(file_path)
        if df is None:
            logging.error("Skipping task due to file load failure.")
            return

        # Split Data
        good_rows, bad_rows = split_dataframe(df, failed_validations_df)

        # Save Files
        save_dataframe(good_rows, GOOD_DATA_FOLDER, f"good_{file_name}")
        save_dataframe(bad_rows, BAD_DATA_FOLDER, f"bad_{file_name}")

        # Delete original file
        delete_file(file_path)


    t1 = read_data(RAW_DATA)
    t2 = validate_data(t1)
    t3 = save_statistics(t2)
    t4 = send_alert(t2)
    t5 = split_and_save_files(t2)

    t1 >> t2 >> [t3, t4, t5]

first_dag = data_injection()

