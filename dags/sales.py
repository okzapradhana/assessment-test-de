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
    dag_id='sales',
    start_date=datetime(2022, 10, 13),
    end_date=datetime(2022, 10, 15),
    default_args=default_args,
    schedule_interval='15 23 * * *',  # UTC based
    catchup=True,
    tags=[
        'sales',
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

    extract_load_customer = SQLToSQLOperator(
        task_id='extract_load_customer',
        source_conn_id='postgres_conn_id',
        target_conn_id='dwh_conn_id',
        connection_type='postgresql',
        sql='extract_customer_pg.sql',
        target_table_name='raw_customer',
        write_chunk_size=100
    )

    extract_load_invoice = SQLToSQLOperator(
        task_id='extract_load_invoice',
        source_conn_id='postgres_conn_id',
        target_conn_id='dwh_conn_id',
        connection_type='postgresql',
        sql='extract_invoice_pg.sql',
        target_table_name='raw_invoice',
        write_chunk_size=100
    )

    create_table_dim_customer = PostgresOperator(
        task_id='create_table_dim_customer',
        postgres_conn_id='dwh_conn_id',
        sql='create_table_dim_customer.sql'
    )

    create_table_dim_invoice_detail = PostgresOperator(
        task_id='create_table_dim_invoice_detail',
        postgres_conn_id='dwh_conn_id',
        sql='create_table_dim_invoice_detail.sql'
    )

    populate_dim_customer = PostgresOperator(
        task_id='populate_dim_customer',
        postgres_conn_id='dwh_conn_id',
        sql='dim_customer.sql'
    )

    populate_dim_invoice_detail = PostgresOperator(
        task_id='populate_dim_invoice_detail',
        postgres_conn_id='dwh_conn_id',
        sql='dim_invoice_detail.sql'
    )
    
    extract_done = EmptyOperator(
        task_id='extract_done'
    )

    create_table_fact_invoice = PostgresOperator(
        task_id='create_table_fact_invoice',
        postgres_conn_id='dwh_conn_id',
        sql='create_table_fact_invoice.sql'
    )
    
    populate_fact_invoice = PostgresOperator(
        task_id='populate_fact_invoice',
        postgres_conn_id='dwh_conn_id',
        sql='fact_invoice.sql'
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    start >> [extract_load_customer, extract_load_invoice]
    [extract_load_customer, extract_load_invoice] >> extract_done
    
    extract_done >> [create_table_dim_customer, create_table_dim_invoice_detail]
    create_table_dim_customer >> populate_dim_customer
    create_table_dim_invoice_detail >> populate_dim_invoice_detail
    [populate_dim_customer, populate_dim_invoice_detail] >> create_table_fact_invoice
    create_table_fact_invoice >> populate_fact_invoice
    populate_fact_invoice >> end