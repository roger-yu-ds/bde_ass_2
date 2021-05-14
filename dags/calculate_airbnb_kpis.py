import pandas as pd
from pathlib import (
    Path,
    WindowsPath
)
import os
import logging
import pandas as pd
import numpy as np
from datetime import (
    datetime,
    timedelta
)
from psycopg2.extras import execute_values
from airflow import DAG
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

project_dir = Path().cwd()
data_dir = project_dir / 'data'
raw_data_dir = data_dir / 'raw'

dag_default_args = {
    'owner': 'roger_yu',
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
    dag_id='calculate_airbnb_kpis',
    default_args=dag_default_args,
    schedule_interval='@hourly',
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

def _load_listing_data(**kwargs) -> pd.DataFrame:
    """
    Load the csv from the `file_dir`. The filename is expected to in the format
    'yyyy-mm-dd.csv'.
    :param dt:
    :return:
    """
    path = kwargs['file_dir'] / f'small_{kwargs["dt"].strftime("%Y-%m-%d")}.csv'

    logging.info(f'Loading {path}')
    df = pd.read_csv(path)
    logging.info(f'type(df): {type(df)}')
    logging.info(f'df.shape: {df.shape}')

    return df


def _insert_data(**kwargs):
    """

    :param kwargs:
    :return:
    """
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='load_listing_data_task')
    values = df.to_dict('split')['data']
    ps_pg_hook = PostgresHook(postgres_conn_id=kwargs['host'])
    conn_ps = ps_pg_hook.get_conn()
    logging.info(f'type(conn_ps): {type(conn_ps)}')

    insert_sql = """
    INSERT INTO airflow
    """

    df.to_sql(con=conn_ps,
              name=kwargs['table_name'],
              if_exists='append')

    return None


load_listing_data_task = PythonOperator(
    task_id='load_listing_data_task',
    python_callable=_load_listing_data,
    op_kwargs={'file_dir': data_dir,
               'dt': datetime(2021, 4, 10)},
    provide_context=True,
    dag=dag
)

create_psql_table_task = PostgresOperator(
    task_id='create_psql_table_task',
    postgres_conn_id='airflow',
    sql="""
        CREATE TABLE IF NOT EXISTS airflow.airbnb_listing (
            city            VARCHAR,
            datetime_utc    TIMESTAMP,
            datetime_local  TIMESTAMP,
            timezone        VARCHAR,
            temp            DECIMAL(10,2),
            feels_like      DECIMAL(10,2),
            pressure        INT,
            humidity        INT,
            dew_point       DECIMAL(10,2),
            clouds          INT,
            wind_speed      DECIMAL(10,2)
            id                     INT,
            listing_url            VARCHAR,
            scrape_id              INT,
            last_scraped           
            name
            description
            neighborhood_overview
            picture_url

            );
    """,
    dag=dag
)

insert_data_task = PythonOperator(
    task_id='insert_data_task',
    python_callable=_insert_data,
    op_kwargs={'host': 'airflow',
               'table_name': 'airbnb_listing'},
    dag=dag
)

load_listing_data_task >> insert_data_task