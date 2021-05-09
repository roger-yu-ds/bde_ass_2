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

project_dir = Path().cwd().parent
data_dir = project_dir / 'data'
raw_data_dir = data_dir / 'raw'

dag = DAG(
    dag_id='calculate_airbnb_kpis',
    start_date=datetime.now() - timedelta(days=2+4),
    schedule_interval='@hourly',
    catchup=True
)

def _load_listing_data(**kwargs) -> pd.DataFrame:
    """
    Load the csv from the `file_dir`. The filename is expected to in the format
    'yyyy-mm-dd.csv'.
    :param dt:
    :return:
    """
    path = kwargs['file_dir'] / f'{kwargs["dt"].strftime("%Y-%m-%d")}.csv'
    df = pd.read_csv(path)

    return df

load_listing_data_task = PythonOperator(
    task_id='load_listing_data_task',
    python_callable=_load_listing_data,
    op_kwargs={'file_dir': raw_data_dir,
               'dt': datetime(2021, 4, 10)},
    provide_context=True,
    dag=dag
)

load_listing_data_task