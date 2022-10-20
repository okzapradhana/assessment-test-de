from airflow import DAG, configuration
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCheckOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import json

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
    dag_id='flights',
    start_date=datetime(2018, 12, 1),
    end_date=datetime(2020, 3, 31),
    default_args=default_args,
    schedule_interval='@monthly',  # UTC based
    catchup=True,
    tags=[
        'flights',
        'gcs',
        'bigquery',
        'monthly'
    ],
    concurrency=1,
    max_active_runs=1,
    template_searchpath=f'{configuration.get_airflow_home()}/sql'
) as dag:
    start = EmptyOperator(
        task_id='start'
    )

    project_id = '{{ var.value.get("GCLOUD_PROJECT_ID", None) }}'
    dataset_name = '{{ var.value.get("BIGQUERY_FLIGHTS_DATASET_NAME", None) }}'
    table_name = '{{ var.value.get("BIGQUERY_FLIGHTS_TABLE_NAME", "keyword_searches") }}'
    bucket = '{{ var.value.get("FLIGHTS_BUCKET_NAME", None) }}'
    year_month_date_interval_start = f'{{{{ data_interval_start.format("YYYY-MM") }}}}'
    
    with open(f'{configuration.get_airflow_home()}/dags/schemas/keyword_searches.json') as file:
        keyword_searches_table_schema = json.load(file)

    # Reference: https://stackoverflow.com/questions/57082158/google-cloud-storage-python-api-get-blob-information-with-wildcard
    check_file_exists = GCSObjectsWithPrefixExistenceSensor(
        task_id='check_file_exists',
        bucket=bucket,
        google_cloud_conn_id='exploration_gcp_conn_id',
        prefix='flights_tickets_serp' + year_month_date_interval_start,
        mode='poke',
        poke_interval=2,
        timeout=5,
        soft_fail=False
    )

    skip_load_flights_gcs_to_bq = EmptyOperator(
        task_id='skip_load_flights_gcs_to_bq',
        trigger_rule=TriggerRule.ONE_FAILED
    )

    load_flights_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_flights_gcs_to_bq',
        bucket=bucket,
        gcp_conn_id='exploration_gcp_conn_id',
        source_objects=['flights_tickets_serp' + year_month_date_interval_start + '*.csv'],
        destination_project_dataset_table=
            f'{dataset_name}.{table_name}' + '_' + year_month_date_interval_start,
        schema_fields=keyword_searches_table_schema,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    init_insert_table = BigQueryExecuteQueryOperator(
        task_id='init_insert_table',
        sql='append_only_flight_keyword_searches_bq.sql',
        gcp_conn_id='exploration_gcp_conn_id',
        destination_dataset_table=f'{dataset_name}.{table_name}_all_time',
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        location='asia-southeast2',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    insert_to_one_table_keyword_searches = BigQueryExecuteQueryOperator(
        task_id='insert_to_one_table_keyword_searches',
        sql='append_only_flight_keyword_searches_bq.sql',
        gcp_conn_id='exploration_gcp_conn_id',
        destination_dataset_table=f'{dataset_name}.{table_name}_all_time',
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        location='asia-southeast2',
        trigger_rule=TriggerRule.ONE_FAILED
    )

    create_table_if_not_exists = BigQueryCreateEmptyTableOperator(
        task_id='create_table_if_not_exists',
        gcp_conn_id='exploration_gcp_conn_id',
        schema_fields=keyword_searches_table_schema,
        project_id=project_id,
        dataset_id=dataset_name,
        table_id=f'{table_name}_all_time',
        location='asia-southeast2',
        trigger_rule=TriggerRule.ONE_FAILED
    )

    check_keyword_searches_table_exists = BigQueryCheckOperator(
        task_id='check_keyword_searches_table_exists',
        sql='check_table_exist_bq.sql',
        gcp_conn_id='exploration_gcp_conn_id',
        use_legacy_sql=False,
        location='asia-southeast2'
    )

    check_rows_keyword_searches_specific_date = BigQueryCheckOperator(
        task_id='check_rows_keyword_searches_specific_date',
        sql='check_rows_on_specific_date_bq.sql',
        gcp_conn_id='exploration_gcp_conn_id',
        use_legacy_sql=False,
        location='asia-southeast2',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    insert_most_searched_keywords_table = BigQueryExecuteQueryOperator(
        task_id='insert_most_searched_keywords_table',
        sql='most_searched_flights_keyword.sql',
        gcp_conn_id='exploration_gcp_conn_id',
        destination_dataset_table=f'{dataset_name}.most_{table_name}',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        location='asia-southeast2'    
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )    

    start >> check_file_exists >> [load_flights_gcs_to_bq, skip_load_flights_gcs_to_bq] 
    load_flights_gcs_to_bq >> check_keyword_searches_table_exists

    check_keyword_searches_table_exists >> [check_rows_keyword_searches_specific_date, create_table_if_not_exists]
    create_table_if_not_exists >> init_insert_table
    check_rows_keyword_searches_specific_date >> [end, insert_to_one_table_keyword_searches]

    insert_to_one_table_keyword_searches >> insert_most_searched_keywords_table
    init_insert_table >> insert_most_searched_keywords_table
    insert_most_searched_keywords_table >> end
    skip_load_flights_gcs_to_bq >> end