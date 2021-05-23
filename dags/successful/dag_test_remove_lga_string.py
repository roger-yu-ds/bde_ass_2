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
from datetime import (
    datetime, 
	timedelta
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

########################################################
#
#   DAG Settings
#
#########################################################

from airflow import DAG

dag_default_args = {
    'owner': 'BDE_LAB_6',
    'start_date': datetime.now() - timedelta(days=1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='test_remove_lga_string',
    default_args=dag_default_args,
    schedule_interval='@hourly',
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################

query = """
UPDATE raw.test_g02_df
SET "LGA_CODE_2016" = RIGHT("LGA_CODE_2016", 5) 
"""
remove_lga_string = PostgresOperator(
    task_id='remove_lga_string',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#
#   DAG Operator Setup
#
#########################################################

remove_lga_string
