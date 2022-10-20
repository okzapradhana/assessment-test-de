from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List
import pandas as pd
from pandas import DataFrame

class SQLToSQLOperator(BaseOperator):
    template_fields: List[str]= [
        'sql'
    ]

    template_ext: List[str] = [
        '.sql'
    ]
    
    def __init__(
        self,
        source_conn_id:str,
        target_conn_id:str,
        connection_type:str, # {mysql, postgresql},
        sql: str,
        target_table_name: str,
        write_chunk_size: int,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.connection_type = connection_type
        self.sql = sql
        self.write_chunk_size = write_chunk_size
        self.target_table_name = target_table_name

    def execute(self, context):
        # check connection type
        print(f"Connection type is {self.connection_type}")
        if self.connection_type == 'mysql':
            self.mysql_to_postgre()
        elif self.connection_type == 'postgresql':
            self.postgre_to_postgre()
    
    def mysql_to_postgre(self):
        # extract
        hook_source = MySqlHook(mysql_conn_id=self.source_conn_id)
        engine = hook_source.get_sqlalchemy_engine()

        with engine.connect() as conn:
            df = pd.read_sql(self.sql, con=conn)
        
        self.load_to_postgre(df, 'staging', 'replace')

    def postgre_to_postgre(self):
        # extract
        hook_source = PostgresHook(postgres_conn_id=self.source_conn_id)
        engine = hook_source.get_sqlalchemy_engine()

        with engine.connect() as conn:
            df = pd.read_sql(self.sql, con=conn)
        
        self.load_to_postgre(df, 'staging', 'replace')

    
    def load_to_postgre(self, df: DataFrame, schema: str, if_exists: str):
        hook_target = PostgresHook(postgres_conn_id=self.target_conn_id)
        engine_target = hook_target.get_sqlalchemy_engine()

        with engine_target.connect() as conn:
            df.to_sql(
                name=self.target_table_name, 
                con=conn,
                schema='staging',
                chunksize=self.write_chunk_size,
                if_exists='replace',
                index=False
            )