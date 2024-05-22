from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import requests
import json
import pandas as pd

def to_str(df)-> str:
    json_data = df.to_json(orient='records')
    json_string = json.dumps(json_data)
    return json_string

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


dag = DAG(
    'prediction_job',
    default_args=default_args,
    description='A DAG to make scheduled predictions',
    schedule_interval='*/2 * * * *',  # Runs every 2 minutes
    start_date=days_ago(1),
    catchup=False,
)

def check_for_new_data(**kwargs):
    good_data_path = '/home/kuzhalogi/dsp_project/data/good_data'
    processed_files_path = '/home/kuzhalogi/dsp_project/data/processed/processed_files.txt'


    if os.path.exists(processed_files_path):
        with open(processed_files_path, 'r') as f:
            processed_files = f.read().splitlines()
    else:
        processed_files = []

    new_files = [f for f in os.listdir(good_data_path) if f not in processed_files]
    if not new_files:
        return 'skip_prediction'


    kwargs['ti'].xcom_push(key='new_files', value=new_files)

def make_predictions(**kwargs):
    new_files = kwargs['ti'].xcom_pull(key='new_files', task_ids='check_for_new_data')
    good_data_path = '/home/kuzhalogi/dsp_project/data/good_data'
    api_endpoint = 'http://localhost:8000/predict'

    for file_name in new_files:
        file_path = os.path.join(good_data_path, file_name)
        # with open(file_path, 'r') as f:
        #     data = f.read()
        data = pd.read_csv(file_path)
        df_txt = pd.DataFrame(data)
        df=to_str(df_txt)
            
        response = requests.post(api_endpoint, json={"source": 'scheduler', "df":df})
        if response.status_code == 200:
            print(f'Successfully processed {file_name}')
        else:
            print(f'Failed to process {file_name}')

    with open('/home/kuzhalogi/dsp_project/data/processed/processed_files.txt', 'a') as f:
        for file_name in new_files:
            f.write(f"{file_name}\n")

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

skip_prediction_task = PythonOperator(
    task_id='skip_prediction',
    python_callable=lambda: print("No new data to process."),
    dag=dag,
)

check_for_new_data_task >> [make_predictions_task, skip_prediction_task]
