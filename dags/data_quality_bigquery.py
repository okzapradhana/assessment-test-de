from airflow import DAG, configuration
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'okza',
    'depends_on_past': False,
    'email': ['okzamahendra29@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'execution_timeout': timedelta(minutes=10),
}

with DAG(
    dag_id='data_quality_bigquery',
    start_date=datetime(2022, 10, 14),
    end_date=datetime(2022, 10, 15),
    default_args=default_args,
    schedule_interval='00 23 * * *',  # UTC based
    catchup=True,
    tags=[
        'csv',
        'great_expectations',
        'bigquery',
        'daily'
    ],
    concurrency=1,
    max_active_runs=1,
    template_searchpath=f'{configuration.get_airflow_home()}/scripts'
) as dag:
    
    start = EmptyOperator(
        task_id='start'
    )

    copy_csv_to_gcs_bucket = BashOperator(
        task_id='copy_csv_to_gcs_bucket',
        bash_command='upload_to_gcs.sh'
    )

    gcs_to_bq = BashOperator(
        task_id='gcs_to_bq',
        bash_command='gcs_to_bq.sh'
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> copy_csv_to_gcs_bucket  >> gcs_to_bq >> end