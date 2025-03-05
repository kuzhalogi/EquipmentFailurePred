import os
import glob
from dag_utils import *
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}


dag = DAG(
    'prediction_job',
    default_args=default_args,
    description='A DAG to make scheduled predictions',
    schedule_interval='*/2 * * * *',  # Runs every 2 minutes
    catchup=False,
)


good_data = GOOD_DATA_FOLDER
PROCESSED_FILES = set()
api_endpoint = API_PREDICT_ENDPOINT


def check_for_new_data(**kwargs):
    # Get list of files in the good_data folder
    all_files = set(glob.glob(os.path.join(good_data, '*.csv')))
    
    # Find new files that haven't been processed yet
    new_files = all_files - PROCESSED_FILES
    
    if not new_files:
        # No new files, skip the DAG run
        raise AirflowSkipException("No new files found. Skipping DAG run.")
    
    # Pass the list of new files to the next task
    kwargs['ti'].xcom_push(key='new_files', value=list(new_files))


def make_predictions(**kwargs):
    new_files = kwargs['ti'].xcom_pull(key='new_files', task_ids='check_for_new_data')
    for file_name in new_files:
        result = predict_new_data(file_name)
        if result:
            logging.info(f"Made Predictions on File {file_name} !")
        else:
            logging.info(f"No Predictions made on File {file_name}")
    

check_for_new_data_task = PythonOperator(
    task_id='check_for_new_data',
    provide_context=True,
    python_callable=check_for_new_data,
    dag=dag,
)


make_predictions_task = PythonOperator(
    task_id='make_predictions',
    provide_context=True,
    python_callable=make_predictions,
    dag=dag,
)


check_for_new_data_task >> make_predictions_task
