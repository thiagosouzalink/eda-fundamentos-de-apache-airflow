from datetime import datetime
import logging 
import requests

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.python import PythonOperator

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

# extract
def extract_bitcoin():
    return requests.get(API).json()["bitcoin"]

# process
def process_bitcoin(ti: TaskInstance):
    response = ti.xcom_pull(task_ids="extract_bitcoin_from_api")
    logging.info(response)
    processed_data = {
        "usd": response["usd"],
        "change": response["usd_24h_change"]
    }
    ti.xcom_push(key="processed_data", value=processed_data)

# store
def store_bitcoin(ti: TaskInstance):
    data = ti.xcom_pull(task_ids="process_bitcoin", key="processed_bitcoin")
    logging.info(PendingDeprecationWarning)

with DAG(
    dag_id="cls_bitcoin",
    schedule="@daily",
    start_date=datetime(2025, 5, 16),
    catchup=False
):
    # task 1
    extract_bitcoin_from_api = PythonOperator(
        task_id="extract_bitcoin_from_api",
        python_callable=extract_bitcoin
    )
    
    # task 2
    process_bitcoin_from_api = PythonOperator(
        task_id="process_bitcoin_from_api",
        python_callable=process_bitcoin
    )
    
    # task 1
    store_bitcoin_from_api = PythonOperator(
        task_id="store_bitcoin_from_api",
        python_callable=store_bitcoin
    )
    
    # set dependencies
    extract_bitcoin_from_api >> process_bitcoin_from_api >> store_bitcoin_from_api