from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import random
import great_expectations as ge
import json

# Define the raw-data directory
RAW_DATA_DIR = "../raw-data"
GOOD_DATA_DIR = "../good-data"
BAD_DATA_DIR = "../bad-data"

# Default args for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define the DAG
with DAG(
    "data_ingestion_pipeline",
    default_args=default_args,
    description="A DAG for data ingestion and validation",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 16),
    catchup=False,
) as dag:

    def read_data():
        # List all files in raw data directory
        files = os.listdir(RAW_DATA_DIR)
        if not files:
            raise FileNotFoundError("No files found in the raw-data folder.")
        
        # Choose a random file
        file_name = random.choice(files)
        file_path = os.path.join(RAW_DATA_DIR, file_name)

        return file_path

    def validate_data(**kwargs):
        # Get the file path from the previous task
        file_path = kwargs["ti"].xcom_pull(task_ids="read_data")

        # Read the CSV into a DataFrame
        df = pd.read_csv(file_path)
        ge_df = ge.from_pandas(df)

        # Load expectation suite
        context = ge.data_context.DataContext()
        suite = context.get_expectation_suite("milling_machine_data_quality")

        # Validate the data
        validation_results = ge_df.validate(expectation_suite=suite)

        # Save validation report
        context.build_data_docs()
        validation_report = context.get_docs_sites_urls()[0]["site_url"]

        # Pass validation results and report link for the next task
        return {
            "validation_results": validation_results,
            "validation_report": validation_report,
            "file_path": file_path,
        }

    def save_statistics(**kwargs):
        # Get validation results from the previous task
        validation_data = kwargs["ti"].xcom_pull(task_ids="validate_data")
        validation_results = validation_data["validation_results"]
        file_path = validation_data["file_path"]

        # Extract statistics
        total_rows = len(pd.read_csv(file_path))
        invalid_rows = sum(not result["success"] for result in validation_results["results"])
        valid_rows = total_rows - invalid_rows

        stats = {
            "file_name": os.path.basename(file_path),
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "error_rate": (invalid_rows / total_rows) * 100,
            "errors": json.dumps(
                {result["expectation_config"]["expectation_type"]: result["success"] for result in validation_results["results"]}
            ),
            "processed_at": datetime.now().isoformat(),
        }

        # Save stats to a database or JSON (simulated here by printing)
        print("Statistics:", stats)

        # Pass statistics for next task
        return stats

    # Define tasks
    read_data_task = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
    )

    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True,
    )

    save_statistics_task = PythonOperator(
        task_id="save_statistics",
        python_callable=save_statistics,
        provide_context=True,
    )

    # Define task dependencies
    read_data_task >> validate_data_task >> save_statistics_task
