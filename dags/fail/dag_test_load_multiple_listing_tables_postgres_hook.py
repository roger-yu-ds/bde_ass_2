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
# TODO: refactor to a module
def stringify_columns(fields: pd.Series,
                      dtypes: pd.Series = None,
                      keys: pd.Series = None) -> str:
    """
    Returns a SQL query to create a table
    :param fields:
    :param dtypes:
    :param keys:
    :return:
    """

    dtype_mapping = {'integer': 'bigint',
                     'bigint': 'bigint',
                     'text': 'text',
                     'float': 'float',
                     'numeric': 'float',
                     'date': 'date',
                     'datetime': 'date',
                     np.nan: 'character',
                     'boolean [t=true; f=false]': 'boolean',
                     'boolean': 'boolean',
                     'string': 'text',
                     'json': 'text',
                     'currency': 'money'}

    if dtypes is not None:
        col_list = [f'    {field} {dtype_mapping[dtype]} PRIMARY KEY'
                    if key
                    else f'    {field} {dtype_mapping[dtype]}'
                    for field, dtype, key
                    in zip(fields, dtypes, keys)]
        result = ', \n'.join(col_list)
    else:
        col_list = [f'    {field}' for field in fields]
        result = ', \n'.join(col_list)

    return result

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
    dag_id='test_load_multiple_listing_tables',
    default_args=dag_default_args,
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#   Custom Logics for Operator
#########################################################

project_dir = Path.cwd()

def _load_from_file(dir: WindowsPath = project_dir / 'data/raw',
                    schema: str = 'raw',
                    **kwargs) -> pd.DataFrame:
    """
    Loads the data from file, drops some null rows, add `execution_date` and
    then upload to Postgres.
    :param dir: The directory that contains the .gz files.
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
    df = df.filter(items=fields).dropna(subset=subset, how='any')
    logging.info(f'df.shape: {df.shape}')
    # Add `execution_date` to be used with `id` as a composite primary key
    df.loc[: 'execution_date'] = datetime(year, month, day)

    return df


def _upload_listing_to_postgres(postgres_conn_id: str,
                                schema: str = 'raw',
                                **kwargs) -> None:
    """
    Uploads the data to the
    :param kwargs:
    :return:
    """
    df = kwargs['ti'].xcom_pull(task_ids='load_from_file')
    execution_date = kwargs['execution_date'].strftime('%Y%m')
    table_name = f'airbnb_listing_{execution_date}'

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    logging.info(f'pg_hook: {pg_hook}')
    logging.info(f'postgres_conn_id: {postgres_conn_id}')
    conn_pg_hook = pg_hook.get_conn()

    # Create table
    query_create = f"""
    CREATE TABLE {schema}.{table_name} ( 
        id BIGINT, 
      	execution_date TIMESTAMP WITHOUT TIME ZONE,
        listing_url TEXT, 
        scrape_id BIGINT, 
        last_scraped TEXT, 
        name TEXT, 
        description TEXT, 
        neighborhood_overview TEXT, 
        picture_url TEXT, 
        host_id BIGINT, 
        host_url TEXT, 
        host_name TEXT, 
        host_since TEXT, 
        host_location TEXT, 
        host_about TEXT, 
        host_response_time TEXT, 
        host_response_rate TEXT, 
        host_acceptance_rate TEXT, 
        host_is_superhost TEXT, 
        host_thumbnail_url TEXT, 
        host_picture_url TEXT, 
        host_neighbourhood TEXT, 
        host_listings_count FLOAT(53), 
        host_total_listings_count FLOAT(53), 
        host_verifications TEXT, 
        host_has_profile_pic TEXT, 
        host_identity_verified TEXT, 
        neighbourhood TEXT, 
        neighbourhood_cleansed TEXT, 
        neighbourhood_group_cleansed FLOAT(53), 
        latitude FLOAT(53), 
        longitude FLOAT(53), 
        property_type TEXT, 
        room_type TEXT, 
        accommodates BIGINT, 
        bathrooms FLOAT(53), 
        bathrooms_text TEXT, 
        bedrooms FLOAT(53), 
        beds FLOAT(53), 
        amenities TEXT, 
        price TEXT, 
        minimum_nights BIGINT, 
        maximum_nights BIGINT, 
        minimum_minimum_nights BIGINT, 
        maximum_minimum_nights BIGINT, 
        minimum_maximum_nights BIGINT, 
        maximum_maximum_nights BIGINT, 
        minimum_nights_avg_ntm FLOAT(53), 
        maximum_nights_avg_ntm FLOAT(53), 
        calendar_updated FLOAT(53), 
        has_availability TEXT, 
        availability_30 BIGINT, 
        availability_60 BIGINT, 
        availability_90 BIGINT, 
        availability_365 BIGINT, 
        calendar_last_scraped TEXT, 
        number_of_reviews BIGINT, 
        number_of_reviews_ltm BIGINT, 
        number_of_reviews_l30d BIGINT, 
        first_review TEXT, 
        last_review TEXT, 
        review_scores_rating FLOAT(53), 
        review_scores_accuracy FLOAT(53), 
        review_scores_cleanliness FLOAT(53), 
        review_scores_checkin FLOAT(53), 
        review_scores_communication FLOAT(53), 
        review_scores_location FLOAT(53), 
        review_scores_value FLOAT(53), 
        license FLOAT(53), 
        instant_bookable TEXT, 
        calculated_host_listings_count BIGINT, 
        calculated_host_listings_count_entire_homes BIGINT, 
        calculated_host_listings_count_private_rooms BIGINT, 
        calculated_host_listings_count_shared_rooms BIGINT, 
        reviews_per_month FLOAT(53),
        PRIMARY KEY(id, execution_date)
        )
    """
    # Create the table
    conn_string = get_connection_string(schema=schema, db_name='airflow')
    engine = sa.create_engine(conn_string)
    engine.connect().execute(query_create)

    # Insert the values
    values = df.to_dict('split')['data']
    columns = stringify_columns(df.columns)
    query_insert = f"""
    INSERT INTO {schema}.{table_name} ({columns}) VALUES %s
    """
    result = execute_values(conn_pg_hook.cursor(),
                            query_insert,
                            values,
                            page_size=len(df))
    conn_pg_hook.commit()


load_from_file = PythonOperator(
    task_id='load_from_file',
    python_callable=_load_from_file,
    dag=dag
)

upload_listing_to_postgres = PythonOperator(
    task_id='upload_listing_to_postgres',
    python_callable=_upload_listing_to_postgres,
    op_kwargs={
        'postgres_conn_id': 'postgres_airflow'
    },
    dag=dag
)

#########################################################
#
#   DAG Operator Setup
#
#########################################################

load_from_file >> upload_listing_to_postgres
