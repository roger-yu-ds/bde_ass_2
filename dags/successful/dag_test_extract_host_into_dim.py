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
    dag_id='test_extract_host_into_dim',
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
SELECT
  host_id
, MAX(host_url) AS host_url
, MAX(host_name) AS host_name
, MAX(host_since) AS host_since
, MAX(host_location) AS host_location
, MAX(host_about) AS host_about
, MAX(host_response_time) AS host_response_time
, MAX(host_response_rate) AS host_response_rate
, MAX(host_acceptance_rate) AS host_acceptance_rate
, MAX(host_is_superhost) AS host_is_superhost
, MAX(host_thumbnail_url) AS host_thumbnail_url
, MAX(host_picture_url) AS host_picture_url
, MAX(host_listings_count) AS host_listings_count
, MAX(host_total_listings_count) AS host_total_listings_count
, MAX(host_verifications) AS host_verifications
, MAX(host_has_profile_pic) AS host_has_profile_pic
, MAX(host_identity_verified) AS host_identity_verified
INTO star.test_dim_host
FROM raw.test_listing_df
GROUP BY 1
"""
extract_host_into_dim = PostgresOperator(
    task_id='extract_host_into_dim',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#
#   DAG Operator Setup
#
#########################################################

extract_host_into_dim