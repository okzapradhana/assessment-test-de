from airflow import DAG, configuration
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from custom_operator.SQLToSQLOperator import SQLToSQLOperator
from airflow.utils.trigger_rule import TriggerRule

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
    dag_id='human_resource',
    start_date=datetime(2022, 10, 13),
    end_date=datetime(2022, 10, 15),
    default_args=default_args,
    schedule_interval='15 23 * * *',  # UTC based
    catchup=True,
    tags=[
        'human_resource',
        'postgres',
        'daily',
    ],
    concurrency=1,
    max_active_runs=1,
    template_searchpath=f'{configuration.get_airflow_home()}/sql'
) as dag:
    start = EmptyOperator(
        task_id='start'
    )

    extract_load_department = SQLToSQLOperator(
        task_id='extract_load_department',
        source_conn_id='mysql_conn_id',
        target_conn_id='dwh_conn_id',
        connection_type='postgresql',
        sql='extract_departments_hr.sql',
        target_table_name='raw_department',
        write_chunk_size=100
    )

    extract_load_job = SQLToSQLOperator(
        task_id='extract_load_job',
        source_conn_id='mysql_conn_id',
        target_conn_id='dwh_conn_id',
        connection_type='postgresql',
        sql='extract_jobs_hr.sql',
        target_table_name='raw_job',
        write_chunk_size=100
    )

    extract_load_employee = SQLToSQLOperator(
        task_id='extract_load_employee',
        source_conn_id='mysql_conn_id',
        target_conn_id='dwh_conn_id',
        connection_type='postgresql',
        sql='extract_employee_hr.sql',
        target_table_name='raw_employee',
        write_chunk_size=100
    )

    create_table_dim_department = PostgresOperator(
        task_id='create_table_dim_department',
        postgres_conn_id='dwh_conn_id',
        sql='create_table_dim_department.sql'
    )

    create_table_dim_job = PostgresOperator(
        task_id='create_table_dim_job',
        postgres_conn_id='dwh_conn_id',
        sql='create_table_dim_job.sql'
    )

    create_table_dim_employee = PostgresOperator(
        task_id='create_table_dim_employee',
        postgres_conn_id='dwh_conn_id',
        sql='create_table_dim_employee.sql'
    )

    populate_dim_department = PostgresOperator(
        task_id='populate_dim_department',
        postgres_conn_id='dwh_conn_id',
        sql='dim_department.sql'
    )

    populate_dim_job = PostgresOperator(
        task_id='populate_dim_job',
        postgres_conn_id='dwh_conn_id',
        sql='dim_job.sql'
    )

    populate_dim_employee = PostgresOperator(
        task_id='populate_dim_employee',
        postgres_conn_id='dwh_conn_id',
        sql='dim_employee.sql'
    )
    
    extract_done = EmptyOperator(
        task_id='extract_done'
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    start >> [extract_load_department, extract_load_job, extract_load_employee]
    [extract_load_department, extract_load_job, extract_load_employee] >> extract_done
    
    extract_done >> [create_table_dim_department, create_table_dim_job, create_table_dim_employee]

    create_table_dim_department >> populate_dim_department
    create_table_dim_job >> populate_dim_job
    create_table_dim_employee >> populate_dim_employee

    [populate_dim_department, populate_dim_job, populate_dim_employee] >> end
    