import pandas as pd
from pathlib import (
    Path,
    WindowsPath
)
import os
import sqlalchemy as sa
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import (
    datetime,
    timedelta
)
from dateutil.relativedelta import relativedelta
from psycopg2.extras import execute_values
from airflow import DAG
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#########################################################
#   Functions
#########################################################
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

project_dir = Path().cwd()
data_dir = project_dir / 'data'
raw_data_dir = data_dir / 'raw'
conn_string = get_connection_string()
engine = sa.create_engine(conn_string)

#########################################################
#   Set up DAG
#########################################################

dag_default_args = {
    'owner': 'roger_yu',
    'start_date': datetime.now() - relativedelta(months=3),
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
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#   Task: Create schemas
#########################################################
schema_list = [
    'raw',
    'star',
    'data_mart',
	'meta'
]
task_dict_create_schemas = {}
for schema in schema_list:
    query = f'CREATE SCHEMA IF NOT EXISTS {schema}'

    task_dict_create_schemas[schema] = PostgresOperator(
        task_id=f'create_schema_{schema}',
        postgres_conn_id='postgres_airflow',
        sql=query,
        dag=dag
    )

#########################################################
#   Tasks: Load listing data from files
#########################################################

def _load_listing_from_file(dir: WindowsPath = project_dir / 'data/raw',
                            **kwargs) -> pd.DataFrame:
    """
    Loads the data from file, drops some null rows, add `execution_date` and
    then upload to Postgres.
    :param dir: The directory that contains the .gz files.
    :param kwargs: kwargs from the Airflow operator
    :return:
    """
    year, month, day = [int(e) for e in kwargs['ds'].split('-')]
    extension = '.gz'
    logging.info(f'dir.exists(): {dir.exists()}')
    # TODO: Potential branching here if the file does not exist or if the month
    # has already been processed.
    try:
        path = next(dir.glob(f'{year}-{str(month).zfill(2)}*.gz'))
    except StopIteration:
        print('File does not exist')

    logging.info(f'path: {path}')
    df = pd.read_csv(path, compression='gzip')

    fields = [
        'id',
        'listing_url',
        'scrape_id',
        'last_scraped',
        'name',
        'summary',
        'space',
        'description',
        'experiences_offered',
        'neighborhood_overview',
        'notes',
        'transit',
        'access',
        'interaction',
        'house_rules',
        'thumbnail_url',
        'medium_url',
        'picture_url',
        'xl_picture_url',
        'host_id',
        'host_url',
        'host_name',
        'host_since',
        'host_location',
        'host_about',
        'host_response_time',
        'host_response_rate',
        'host_acceptance_rate',
        'host_is_superhost',
        'host_thumbnail_url',
        'host_picture_url',
        'host_neighbourhood',
        'host_listings_count',
        'host_total_listings_count',
        'host_verifications',
        'host_has_profile_pic',
        'host_identity_verified',
        'street',
        'neighbourhood',
        'neighbourhood_cleansed',
        'neighbourhood_group_cleansed',
        'city',
        'state',
        'zipcode',
        'market',
        'smart_location',
        'country_code',
        'country',
        'latitude',
        'longitude',
        'is_location_exact',
        'property_type',
        'room_type',
        'accommodates',
        'bathrooms',
        'bedrooms',
        'beds',
        'bed_type',
        'amenities',
        'square_feet',
        'price',
        'weekly_price',
        'monthly_price',
        'security_deposit',
        'cleaning_fee',
        'guests_included',
        'extra_people',
        'minimum_nights',
        'maximum_nights',
        'minimum_minimum_nights',
        'maximum_minimum_nights',
        'minimum_maximum_nights',
        'maximum_maximum_nights',
        'minimum_nights_avg_ntm',
        'maximum_nights_avg_ntm',
        'calendar_updated',
        'has_availability',
        'availability_30',
        'availability_60',
        'availability_90',
        'availability_365',
        'calendar_last_scraped',
        'number_of_reviews',
        'number_of_reviews_ltm',
        'first_review',
        'last_review',
        'review_scores_rating',
        'review_scores_accuracy',
        'review_scores_cleanliness',
        'review_scores_checkin',
        'review_scores_communication',
        'review_scores_location',
        'review_scores_value',
        'requires_license',
        'license',
        'jurisdiction_names',
        'instant_bookable',
        'is_business_travel_ready',
        'cancellation_policy',
        'require_guest_profile_picture',
        'require_guest_phone_verification',
        'calculated_host_listings_count',
        'calculated_host_listings_count_entire_homes',
        'calculated_host_listings_count_private_rooms',
        'calculated_host_listings_count_shared_rooms',
        'reviews_per_month',
    ]
    # Drop rows
    subset = [
        'host_name',
        'host_id',
        'latitude',
        'longitude',
    ]
    logging.info(f'df.shape: {df.shape}')
    logging.info(f'df.loc[0, "price"]: {df.loc[0, "price"]}')
    df = df.filter(items=fields).dropna(subset=subset, how='any')
    logging.info(f'df.shape: {df.shape}')

    # Add `execution_date` to be used with `id` as a composite primary key
    df = df.assign(execution_date=datetime(year, month, day))

    # Change `price` to numeric
    logging.info(f'df.loc[0, "price"]: {df.loc[0, "price"]}')
    logging.info(f'type(df.loc[0, "price"]): {type(df.loc[0, "price"])}')
    df.loc[:, 'price'] = (
        df['price']
        .str.replace(pat='$', repl='', regex=False)
        .str.replace(pat=',', repl='', regex=False)
        .str.replace(pat='.', repl='', regex=False)
        .astype(int)
    )
    return df

task_load_listing_from_file = PythonOperator(
    task_id='load_listing_from_file',
    python_callable=_load_listing_from_file,
    dag=dag
)

#########################################################
#   Tasks: Upload the airbnb listing data to Postgres
#########################################################

def _upload_listing_to_postgres(schema: str = 'raw',
                                **kwargs) -> None:
    """
    Uploads the data to the
    :param kwargs:
    :return:
    """
    df = kwargs['ti'].xcom_pull(task_ids='load_listing_from_file')
    execution_date = kwargs['execution_date'].strftime('%Y%m')
    table_name = f'airbnb_listing_{execution_date}'

    # Connect to Postgres
    conn_string = get_connection_string(db_name='airflow')
    logging.info(f'conn_string: {conn_string}')
    engine = sa.create_engine(conn_string)
    df.to_sql(con=engine,
              name=table_name,
              schema=schema,
              if_exists='replace',
              index=False)
    # query = 'SELECT schema_name FROM information_schema.schemata;'
    # logging.info(f'Schema list: {engine.connect().execute(query)}')


task_upload_listing_to_postgres = PythonOperator(
    task_id='upload_listing_to_postgres',
    python_callable=_upload_listing_to_postgres,
    op_kwargs={
        'postgres_conn_id': 'postgres_airflow'
    },
    dag=dag
)

#########################################################
#   Load census tables and shapefile from disk
#########################################################
def _load_from_file(dir: WindowsPath = project_dir / 'data/raw',
                    **kwargs) -> pd.DataFrame:
    """
    Loads the data from file and returns a dataframe. This step doesn't require
    any preprocessing.
    :param dir: The directory that contains the files.
    :param kwargs: kwargs from the Airflow operator
    :return: Pandas dataframe
    """
    filename = kwargs['filename']
    path = dir / filename
    if path.suffix == '.csv':
        df = pd.read_csv(path)
    elif path.suffix == '.shp':
        df = gpd.read_file(path)

    return df

paths = [
    raw_data_dir / '2016Census_G01_NSW_LGA.csv',
    raw_data_dir / '2016Census_G02_NSW_LGA.csv',
    raw_data_dir / 'shapefile/LGA_2016_AUST.shp'
]

task_dict_load_files = {}
for path in paths:
    task_dict_load_files[path.stem] = PythonOperator(
        task_id=f'load_from_file_{path.stem}',
        python_callable=_load_from_file,
        op_kwargs={'dir': path.parent,
                   'filename': path.name},
        dag=dag
    )


#########################################################
#   Upload census tables and shapefile to Postgres
#########################################################
def _upload_to_postgres(task_id: str,
                        engine: sa.engine.base.Engine,
                        table_name: str,
                        schema: str,
                        **kwargs) -> None:
    """
    Upload tables to Postgres
    :param task_id: the source `task_id` from which to extract the data
    :param engine: sqlalchemy engine to connect to the database
    :param table_name: The table name of the table to be created
    :param schema: The Postgres schema into which the tables will be uploaded
    :param kwargs: kwargs from the Airflow operator
    :return:
    """
    ti = kwargs['ti']
    logging.info(f'task_id: {task_id}')
    df = ti.xcom_pull(task_ids=task_id)
    logging.info(f'type(df): {type(df)}')
    logging.info(f'df.shape: {df.shape}')
    logging.info(f'df.info(): {df.info()}')


    # Check for geopandas dataframe
    if 'LGA_2016_AUST' in task_id:
        df.to_postgis(con=engine,
                      name=table_name,
                      schema=schema,
                      if_exists='replace',
                      index=False)
    else:
        df.to_sql(con=engine,
                  name=table_name,
                  schema=schema,
                  if_exists='replace',
                  index=False)

task_dict_upload_files_to_postgres = {}

for path in paths:
    task_dict_upload_files_to_postgres[path.stem] = PythonOperator(
        task_id=f'upload_to_postgres_{path.stem}',
        python_callable=_upload_to_postgres,
        op_kwargs={'task_id': f'load_from_file_{path.stem}',
                   'engine': engine,
                   'table_name': path.stem,
                   'schema': 'star'},
        dag=dag
    )

# The loading of the three dimension tables;
# - G01
# - G02
# - Shapefile
for load_task, upload_task in zip(list(task_dict_load_files.values()), list(task_dict_upload_files_to_postgres.values())):
    load_task >> upload_task

#########################################################
#   DAG organisation
#########################################################

task_load_listing_from_file >> task_upload_listing_to_postgres
task_dict_create_schemas['raw'] >> task_upload_listing_to_postgres
task_dict_create_schemas['star'] >> list(task_dict_upload_files_to_postgres.values())
