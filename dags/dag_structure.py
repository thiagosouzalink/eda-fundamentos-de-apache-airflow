"""
DAG Skeleton

- Imports
- Connections & Variables
- Default Arguments
- DAG Definition
- Task Declaration
- Task Dependencies
- DAG Instantiation
"""
# Imports
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.standard.operators.empty import EmptyOperator


# Default Arguments
default_args = {
    "owner": "Thiago Souza",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# DAG Definition
@dag(
    dag_id="dag_structure",
    start_date=datetime(year=2025, month=5, day=1),
    max_active_runs=1,
    schedule=timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=["first", "dag"]
)
def init():
    # Task Declaration
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Task Dependencies
    start >> end
    

# DAG Instantiation
dag = init()