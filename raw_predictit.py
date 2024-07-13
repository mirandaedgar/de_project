import requests
import json
import boto3
import airflow
from airflow import DAG
from airflow.operators.dummy.operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import datetime

start_date = airflow.utils.dates.days_ago(2) #

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["edgar@mirandaedgar.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

def json_parser(url, file_name, bucket):
    print('starting...')
    response = requests.request("GET", url)
    json_data=response.json()

    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)

    print('complete')
    s3 = boto3.client('s3')
    s3.upload_file(file_name, bucket,f"predictit/{file_name}")

with DAG(
    "raw_predictit",
    default_args=default_args,
    description="Daily Predictit Data Pull",
    schedule_interval=datetime.timedelta(days=1),
    start_date=start_date,
    catchup=False,
    tags=["predictit"]
) as dag:
    

    extract_predictit = PythonOperator(
    task_id='extract_predictit',
    python_callable=json_parser,
    op_kwargs={
                'url':"https://www.predictit.org/api/marketdata/all/",
                'file_name':'predictit_market.json',
                'bucket':"data-bfd"},
        dag=dag
    )


    isready = DummyOperator(task_id='ready')

    extract_predictit >> isready