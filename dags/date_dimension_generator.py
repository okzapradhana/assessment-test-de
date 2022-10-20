from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG, configuration
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
    dag_id='date_dimension_generator',
    start_date=datetime(2022, 10, 14),
    end_date=datetime(2022, 10, 15),
    default_args=default_args,
    schedule_interval='00 23 * * *',  # UTC based
    catchup=True,
    tags=[
        'dimension',
        'date',
        'daily',
    ],
    concurrency=1,
    max_active_runs=1,
    template_searchpath=f'{configuration.get_airflow_home()}/sql'
) as dag:
    
    create_date_dimension = PostgresOperator(
        task_id='create_date_dimension',
        postgres_conn_id='dwh_conn_id',
        sql='dim_date.sql'
    )

    create_date_dimension