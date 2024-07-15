import requests
import pandas as pd
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

def json_scraper(url, json_file_name, markets_file_name, contracts_file_name, bucket):
    print('starting...')
    response = requests.request("GET", url)
    json_data=response.json()
    markets_data = pd.json_normalize(json_data, 'markets')
    contracts_data = pd.json_normalize(json_data['markets'], 'contracts', ['id'], record_prefix='contracts_')

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)
    
    markets_data.to_csv(markets_file_name, index=False)
    contracts_data.to_csv(contracts_file_name, index=False)

    print('complete')
    s3 = boto3.client('s3')
    s3.upload_file(json_file_name, bucket,f"predictit/{json_file_name}")
    s3.upload_file(markets_file_name, bucket,f"predictit/{markets_file_name}")
    s3.upload_file(contracts_file_name, bucket,f"predictit/{contracts_file_name}")

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
    python_callable=json_scraper,
    op_kwargs={
                'url':"https://www.predictit.org/api/marketdata/all/",
                'json_file_name': 'predictit_market.json',
                'markets_file_name':'predictit_markets.csv',
                'contracts_file_name':'predictit_contracts.csv',
                'bucket':"data-bfd"},
        dag=dag
    )


    isready = DummyOperator(task_id='ready')

    extract_predictit >> isready