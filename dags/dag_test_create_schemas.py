########################################################
#
#   Introduction
#
#########################################################
# Extracting the numerical part of the LGA code for
# `2016Census_G01_NSW_LGA.csv` and `2016Census_G02_NSW_LGA.csv`, i.e.
# 'LGA10050' -> '10050', because the shapefile only has numbers.


import os
import logging
import pandas as pd
import numpy as np
import sqlalchemy as sa
# from dotenv import(
#     find_dotenv,
#     load_dotenv
# )
from dateutil.relativedelta import relativedelta
from pathlib import (
    Path,
    WindowsPath
)
from datetime import (
    datetime, 
	timedelta
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from psycopg2.extras import execute_values

# from src.data.database import (
#     get_connection_string
# )
# from src.utils.utils import (
#     stringify_columns
# )

########################################################
#   Functions
#########################################################
#########################################################
#   Connect to Postgres
#########################################################
# TODO: refactor to a module
def get_connection_string(schema: str=None,
                          db_name: str='airflow') -> str:
    """

    :return: a connection string
    """
    # load_dotenv(find_dotenv())
    # POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
    # POSTGRES_PORT = os.environ.get('POSTGRES_PORT')
    # POSTGRES_USERNAME = os.environ.get('POSTGRES_USERNAME')
    # POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
    # POSTGRES_DB = os.environ.get('POSTGRES_DB')
    POSTGRES_HOST = 'postgres'
    POSTGRES_PORT = 5432
    POSTGRES_USERNAME = 'airflow'
    POSTGRES_PASSWORD = 'airflow'
    POSTGRES_DB = 'airflow'

    if schema is not None:
        conn = f'postgresql+psycopg2://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@' \
               f'{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}?' \
               f'currentSchema={schema}'
    else:
        conn = f'postgresql+psycopg2://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@' \
               f'{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}'

    return conn

########################################################
#   DAG Settings
#########################################################

from airflow import DAG

dag_default_args = {
    'owner': 'BDE_LAB_6',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='test_create_schemas',
    default_args=dag_default_args,
    schedule_interval='@once',
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#   Custom Logics for Operator
#########################################################
schema_list = [
    'raw',
    'star',
    'data_mart',
	'meta'
]
task_list_create_schemas = []
for schema in schema_list:
    query = f'CREATE SCHEMA IF NOT EXISTS {schema}'

    task_list_create_schemas.append(
        PostgresOperator(
        task_id=f'create_schema_{schema}',
        postgres_conn_id='postgres_airflow',
        sql=query,
        dag=dag
        )
    )

#########################################################
#   Trigger another DAG
#########################################################
trigger_load_data_dag = TriggerDagRunOperator(
    task_id='trigger_load_data_dag',
    trigger_dag_id="test_load_multiple_listing_tables_pandas",
    dag=dag
)

task_list_create_schemas >> trigger_load_data_dag