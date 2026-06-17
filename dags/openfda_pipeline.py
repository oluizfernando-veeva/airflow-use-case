from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from utils.extract import extract_adverse_events
from utils.transform import transform_data
from utils.load import load_to_postgres

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def branch_on_data(**context):
    data = context['ti'].xcom_pull(task_ids='transform_data')
    return 'load_to_postgres' if data else 'skip'


with DAG(
    dag_id='openfda_adverse_events',
    default_args=default_args,
    description='Hourly ingestion of FDA adverse event reports into PostgreSQL',
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=['openfda', 'pharma', 'etl'],
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_adverse_events,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    branch = BranchPythonOperator(
        task_id='branch_on_data',
        python_callable=branch_on_data,
    )

    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    skip = EmptyOperator(task_id='skip')

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    extract >> transform >> branch >> [load, skip] >> end
