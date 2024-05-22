import logging
import os
import random
import shutil
from datetime import timedelta
import sqlalchemy as sa
import great_expectations as ge
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# Define constants for folder paths
RAW_DATA_FOLDER = "../dsp_project/raw-data"
GOOD_DATA_FOLDER = "../dsp_project/data/good_data"
BAD_DATA_FOLDER = "../dsp_project/data/bad_data"

# Database connection string (replace with actual credentials)
DATABASE_CONN_STR = 'postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>'

def decide_branch(**kwargs):
    # Simulate data quality validation result
    data_quality_issues = random.choice(["none", "some", "all"])
    if data_quality_issues == "none":
        return "split_and_save_data"
    else:
        return ["split_and_save_data", "send_alert", "save_data_errors"]

@dag(
    dag_id='data_injection',
    description='Checks for issues and separates good and bad data from the .csv file',
    tags=['dsp'],
    schedule_interval=timedelta(minutes=5),
    start_date=days_ago(1)  # start_date should not be in the future
)
def data_injection():
    @task
    def read_data():
        files = os.listdir(RAW_DATA_FOLDER)
        file = random.choice(files)
        file_path = os.path.join(RAW_DATA_FOLDER, file)
        return file_path

    @task
    def validate_data(file_path: str):
        # Use Great Expectations for data validation
        context = ge.data_context.DataContext()
        batch_kwargs = {"path": file_path, "datasource": "my_datasource"}
        batch = context.get_batch(batch_kwargs, "your_expectation_suite_name")
        results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch])
        return results

    @task
    def split_and_save_data(file_path: str, validation_result):
        if not os.path.exists(GOOD_DATA_FOLDER):
            os.makedirs(GOOD_DATA_FOLDER)
        if not os.path.exists(BAD_DATA_FOLDER):
            os.makedirs(BAD_DATA_FOLDER)

        if validation_result["success"]:
            # Move file to good data folder
            shutil.move(file_path, os.path.join(GOOD_DATA_FOLDER, os.path.basename(file_path)))
            return "none"
        else:
            # Split good and bad data based on validation
            data_quality_issues = random.choice(["some", "all"])  # Simulate based on validation
            if data_quality_issues == "some":
                # Split file into good and bad data
                shutil.copy(file_path, os.path.join(GOOD_DATA_FOLDER, os.path.basename(file_path)))
                shutil.copy(file_path, os.path.join(BAD_DATA_FOLDER, os.path.basename(file_path)))
                return "some"
            else:
                # Move file to bad data folder
                shutil.move(file_path, os.path.join(BAD_DATA_FOLDER, os.path.basename(file_path)))
                return "all"

    @task
    def send_alert(validation_result):
        # Generate and send alert using Teams notification
        data_quality_issues = validation_result["data_quality_issues"]
        criticality = "high" if data_quality_issues == "all" else "medium"
        summary = f"Data quality issues detected. Criticality: {criticality}"
        report_link = "http://link.to/your/great_expectations/data_docs"
        message = f"{summary}\nReport: {report_link}"

        # Replace with actual Teams webhook URL
        webhook_url = "https://outlook.office.com/webhook/your-webhook-url"
        requests.post(webhook_url, json={"text": message})

    @task
    def save_data_errors(file_path: str, validation_result):
        # Save detected data problems (statistics) to the database
        engine = sa.create_engine(DATABASE_CONN_STR)
        connection = engine.connect()
        metadata = sa.MetaData()
        data_errors = sa.Table('data_errors', metadata, autoload_with=engine)
        
        error_stats = {
            "timestamp": datetime.utcnow(),
            "file_path": file_path,
            "errors": str(validation_result)
        }
        connection.execute(data_errors.insert(), error_stats)
        connection.close()

    t1 = read_data()
    t2 = validate_data(t1.output)
    branching_task = BranchPythonOperator(
        task_id='branching_task',
        python_callable=decide_branch,
        provide_context=True,
    )

    t3 = split_and_save_data(t1.output, t2.output)
    t4 = send_alert(t2.output)
    t5 = save_data_errors(t1.output, t2.output)

    # Define task dependencies
    t1 >> t2 >> branching_task
    branching_task >> [t3, t4, t5]
    t3 >> [EmptyOperator(task_id='end')]

# Instantiate the DAG
first_dag = data_injection()
