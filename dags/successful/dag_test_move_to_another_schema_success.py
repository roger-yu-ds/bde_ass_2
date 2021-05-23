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
    dag_id='test_move_to_another_schema',
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

# pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'])
# conn_ps = pg_hook.get_conn()
# logging.info(f'type(conn_ps): {type(conn_ps)}')
# logging.info(f'conn_ps: {conn_ps}')
query = """
SELECT *
INTO star.test_dim 
FROM raw.test_listing_df
LIMIT 10
"""
move_to_another_schema = PostgresOperator(
    task_id='move_to_another_schema',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#
#   DAG Operator Setup
#
#########################################################

move_to_another_schema