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
    dag_id='test_join_postgis',
    default_args=dag_default_args,
    schedule_interval='@hourly',
    catchup=True,
    max_active_runs=1,
    concurrency=5,
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
select 
  tld.*
, tsd."LGA_NAME20"
into star.test_listing_with_lga_names 
from raw.test_listing_df tld
left join raw.test_shape_df tsd 
on ST_CONTAINS(st_setsrid(tsd.geometry, 4326), ST_SetSRID(st_point(tld.longitude, tld.latitude), 4326))
;
"""
join_postgis = PostgresOperator(
    task_id='join_postgis',
    postgres_conn_id='postgres_airflow',
    sql=query,
    execution_timeout=timedelta(seconds=1000),
    dag=dag
)

#########################################################
#
#   DAG Operator Setup
#
#########################################################

join_postgis