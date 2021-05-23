import pandas as pd
from typing import Optional
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
postgres_conn_id = 'postgres_airflow'

#########################################################
#   Set up DAG
#########################################################

dag_default_args = {
    'owner': 'roger_yu',
    'start_date': datetime.now() - relativedelta(months=11),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='random',
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
#   Tasks: Create history table
#########################################################
query = f"""CREATE TABLE IF NOT EXISTS meta.history (
    table_name TEXT,
    year_scraped INT,
    month_scraped INT,
    n_rows INT,
    n_cols INT
)
"""
task_create_history_table = PostgresOperator(
    task_id='create_history_table',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#   Tasks: Load listing data from files
#########################################################
def is_loaded(table_name: str, engine: sa.engine.base.Engine) -> bool:
    """
    Check if the data has been loaded
    :param table_name: The name of the table to be checked
    :param engine: The sqlalchemy engine to connect to the database
    :return: True if the entry exists in the `meta.history` table
    """
    query = f"""
    SELECT *
    FROM pg_catalog.pg_tables
    WHERE schemaname != 'pg_catalog' AND 
          schemaname != 'information_schema';
    """
    history_df = pd.read_sql(con=engine,
                             sql=query)
    if table_name in history_df['tablename']:
        result = True
    else:
        result = False

    return result

def update_history(df: pd.DataFrame,
                   year_scraped: int,
                   month_scraped: int,
                   table_name: str,
                   engine: sa.engine.base.Engine) -> None:
    """
    Update the history table of what has been loaded.
    :param df: The dataframe loaded to the `raw` schema
    :param year: The year the data was scraped
    :param month: The month the data was scraped
    :param table_name: The name of the table to be checked
    :param engine: The sqlalchemy engine to connect to the database
    :return:
    """
    df_upload = pd.DataFrame({'table_name': [table_name],
                              'year_scraped': year_scraped,
                              'month_scraped': month_scraped,
                              'n_rows': df.shape[0],
                              'n_cols': df.shape[1]})
    df_upload.to_sql(con=engine,
                     name='history',
                     schema='meta',
                     if_exists='append',
                     index=False)

def _load_listing_from_file(engine: sa.engine.base.Engine,
                            dir: WindowsPath = project_dir / 'data/raw',
                            **kwargs) -> Optional[pd.DataFrame]:
    """
    Loads the data from file, drops some null rows, add `execution_date` and
    then upload to Postgres.
    :param engine: The sqlalchemy engine to connect to the database
    :param dir: The directory that contains the .gz files.
    :param kwargs: kwargs from the Airflow operator
    :return:
    """
    year, month, day = [int(e) for e in kwargs['ds'].split('-')]
    table_name = f'airbnb_listing_{year}{str(month).zfill(2)}'
    logging.info(f'is_loaded: {is_loaded(table_name=table_name, engine=engine)}')
    if not is_loaded(table_name=table_name, engine=engine):
        extension = '.gz'
        logging.info(f'dir.exists(): {dir.exists()}')
        # TODO: Potential branching here if the file does not exist or if the month
        # has already been processed.
        try:
            path = next(dir.glob(f'{year}-{str(month).zfill(2)}*1000.gz'))
        except StopIteration:
            print('File does not exist')

        logging.info(f'path: {path}')
        df = pd.read_csv(path, compression='gzip')

        # These were precalculated to be common across all 12 files.
        fields = [
            'accommodates',
             'availability_30',
             'availability_365',
             'availability_60',
             'availability_90',
             'bathrooms',
             'calculated_host_listings_count',
             'calculated_host_listings_count_entire_homes',
             'calculated_host_listings_count_private_rooms',
             'calculated_host_listings_count_shared_rooms',
             'calendar_last_scraped',
             'calendar_updated',
             'description',
             'first_review',
             'has_availability',
             'host_about',
             'host_acceptance_rate',
             'host_has_profile_pic',
             'host_id',
             'host_identity_verified',
             'host_is_superhost',
             'host_listings_count',
             'host_location',
             'host_name',
             'host_neighbourhood',
             'host_picture_url',
             'host_response_rate',
             'host_response_time',
             'host_since',
             'host_thumbnail_url',
             'host_total_listings_count',
             'host_url',
             'host_verifications',
             'id',
             'instant_bookable',
             'last_review',
             'last_scraped',
             'latitude',
             'license',
             'listing_url',
             'longitude',
             'maximum_maximum_nights',
             'maximum_minimum_nights',
             'maximum_nights_avg_ntm',
             'minimum_maximum_nights',
             'minimum_minimum_nights',
             'minimum_nights_avg_ntm',
             'name',
             'neighborhood_overview',
             'neighbourhood',
             'neighbourhood_cleansed',
             'neighbourhood_group_cleansed',
             'number_of_reviews',
             'number_of_reviews_ltm',
             'picture_url',
             'price',
             'property_type',
             'review_scores_accuracy',
             'review_scores_checkin',
             'review_scores_cleanliness',
             'review_scores_communication',
             'review_scores_location',
             'review_scores_rating',
             'review_scores_value',
             'reviews_per_month',
             'room_type',
             'scrape_id'
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

        # If the price is a string dtype then convert it
        if df['price'].dtype == 'O':
            df.loc[:, 'price'] = (
                df['price']
                .str.replace(pat='$', repl='', regex=False)
                .str.replace(pat=',', repl='', regex=False)
                .str.replace(pat='.', repl='', regex=False)
                .astype(int)
            )
    else:
        df = None
    return df

task_load_listing_from_file = PythonOperator(
    task_id='load_listing_from_file',
    python_callable=_load_listing_from_file,
    op_kwargs={'engine': engine},
    dag=dag
)

#########################################################
#   Tasks: Upload the airbnb listing data to Postgres
#########################################################

def _upload_listing_to_postgres(engine: sa.engine.base.Engine,
                                schema: str = 'raw',
                                **kwargs) -> None:
    """
    Uploads the airbnb listing data to the `raw` schema
    :param engine:
    :param schema:
    :param kwargs:
    :return:
    """
    df = kwargs['ti'].xcom_pull(task_ids='load_listing_from_file')
    year, month, day = [int(e) for e in kwargs['ds'].split('-')]
    if df is not None:
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
        update_history(df=df,
                       year_scraped=year,
                       month_scraped=month,
                       table_name=table_name,
                       engine=engine)

        df.to_sql(con=engine,
                  name='airbnb_listing_latest',
                  schema=schema,
                  if_exists='replace',
                  index=False)


task_upload_listing_to_postgres = PythonOperator(
    task_id='upload_listing_to_postgres',
    python_callable=_upload_listing_to_postgres,
    op_kwargs={'postgres_conn_id': 'postgres_airflow',
               'engine': engine},
    dag=dag
)

#########################################################
#   Load census tables and shapefile from disk
#########################################################
def _load_from_file_upload_to_postgres(
        dir: WindowsPath,
        filename: str,
        task_id: str,
        engine: sa.engine.base.Engine,
        table_name: str,
        schema: str,
        **kwargs) -> None:
    """
    Loads the data from file and returns a dataframe. This step doesn't require
    any preprocessing.
    :param dir: The directory that contains the files.
    :param filename: The filename on disk
    :param task_id: the source `task_id` from which to extract the data
    :param engine: sqlalchemy engine to connect to the database
    :param table_name: The table name of the table to be created
    :param schema: The Postgres schema into which the tables will be uploaded
    :param kwargs: kwargs from the Airflow operator
    :return: Pandas dataframe
    """
    path = dir / filename
    if not is_loaded(table_name, engine):
        if path.suffix == '.csv':
            df = pd.read_csv(path)
            df.to_sql(con=engine,
                      name=table_name,
                      schema=schema,
                      if_exists='replace',
                      index=False)
        elif path.suffix == '.shp':
            df = (
                gpd
                .read_file(path)
                .dropna(subset=['geometry'])
                # For consistency with the census tables
                .rename(columns={'LGA_CODE16': 'LGA_CODE_2016'})
            )
            df.to_postgis(con=engine,
                          name=table_name,
                          schema=schema,
                          if_exists='replace',
                          index=False)
        update_history(df,
                       year_scraped=0,
                       month_scraped=0,
                       table_name=table_name,
                       engine=engine)

paths = [
    raw_data_dir / '2016Census_G01_NSW_LGA.csv',
    raw_data_dir / '2016Census_G02_NSW_LGA.csv',
    raw_data_dir / 'shapefile/LGA_2016_AUST.shp'
]

task_dict_load_from_file_upload_to_postgres = {}
for path in paths:
    task_dict_load_from_file_upload_to_postgres[path.stem] = PythonOperator(
        task_id=f'load_from_file_upload_to_postgres_{path.stem}',
        python_callable=_load_from_file_upload_to_postgres,
        op_kwargs={'dir': path.parent,
                   'filename': path.name,
                   'task_id': f'load_from_file_{path.stem}',
                   'engine': engine,
                   'table_name': path.stem,
                   'schema': 'star'},
        execution_timeout=timedelta(seconds=300),
        dag=dag
    )

#########################################################
#   Format the LGA code in G01 and G02 tables
#########################################################
census_operator_dict = {
    '2016Census_G01_NSW_LGA': None,
    '2016Census_G02_NSW_LGA': None
}

for name, operator in census_operator_dict.items():
    query = f"""
    UPDATE star."{name}" 
    SET "LGA_CODE_2016" = RIGHT("LGA_CODE_2016", 5)
    """
    operator = PostgresOperator(
        task_id=f'format_lga_code_{name}',
        postgres_conn_id=postgres_conn_id,
        sql=query,
        dag=dag
    )
    # DAG organisation
    task_dict_load_from_file_upload_to_postgres[name] >> operator

#########################################################
#   Create fact table for the airbnb listing
#########################################################
# TODO: delete the DROP TABLE command
query = """
DROP TABLE IF EXISTS star.fact_airbnb;

CREATE TABLE IF NOT EXISTS star.fact_airbnb (
	id BIGINT, 
	execution_date TIMESTAMP WITHOUT TIME ZONE,
--	listing_url TEXT, 
--	latitude FLOAT(53), 
--	longitude FLOAT(53), 
--	minimum_nights BIGINT, 
--	maximum_nights BIGINT, 
	minimum_minimum_nights BIGINT, 
	maximum_minimum_nights BIGINT, 
	minimum_maximum_nights BIGINT, 
	maximum_maximum_nights BIGINT, 
	minimum_nights_avg_ntm FLOAT(53), 
	maximum_nights_avg_ntm FLOAT(53), 
--	calendar_updated FLOAT(53), 
	has_availability BOOLEAN, 
	availability_30 BIGINT, 
	availability_60 BIGINT, 
	availability_90 BIGINT, 
	availability_365 BIGINT, 
	calendar_last_scraped TEXT, 
--	number_of_reviews BIGINT, 
--	number_of_reviews_ltm BIGINT, 
--	number_of_reviews_l30d BIGINT, 
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
	instant_bookable BOOLEAN, 
	calculated_host_listings_count BIGINT, 
	calculated_host_listings_count_entire_homes BIGINT, 
	calculated_host_listings_count_private_rooms BIGINT, 
	calculated_host_listings_count_shared_rooms BIGINT, 
	reviews_per_month FLOAT(53),
	"LGA_CODE_2016" TEXT, 
	PRIMARY KEY (id, execution_date)
) 
--PARTITION BY RANGE (execution_date)
"""
task_create_fact_airbnb_table = PostgresOperator(
    task_id='create_fact_airbnb_table',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#   Extract airbnb listing into fact table and add LGA codes
#########################################################
query = """
INSERT INTO star.fact_airbnb
SELECT
    t1.id, 
	t1.execution_date,
--	t1.listing_url, 
--	t1.latitude, 
--	t1.longitude, 
--	t1.minimum_nights, 
--	t1.maximum_nights, 
	t1.minimum_minimum_nights, 
	t1.maximum_minimum_nights, 
	t1.minimum_maximum_nights, 
	t1.maximum_maximum_nights, 
	t1.minimum_nights_avg_ntm, 
	t1.maximum_nights_avg_ntm, 
--	t1.calendar_updated, 
	t1.has_availability::BOOLEAN,
	t1.availability_30, 
	t1.availability_60, 
	t1.availability_90, 
	t1.availability_365, 
	t1.calendar_last_scraped, 
--	t1.number_of_reviews, 
--	t1.number_of_reviews_ltm, 
--	t1.number_of_reviews_l30d, 
	t1.first_review, 
	t1.last_review, 
	t1.review_scores_rating, 
	t1.review_scores_accuracy, 
	t1.review_scores_cleanliness, 
	t1.review_scores_checkin, 
	t1.review_scores_communication, 
	t1.review_scores_location, 
	t1.review_scores_value, 
	t1.license, 
	t1.instant_bookable::BOOLEAN, 
	t1.calculated_host_listings_count, 
	t1.calculated_host_listings_count_entire_homes, 
	t1.calculated_host_listings_count_private_rooms, 
	t1.calculated_host_listings_count_shared_rooms, 
	t1.reviews_per_month,
	t2."LGA_CODE_2016"
FROM raw.airbnb_listing_latest t1
LEFT JOIN star."LGA_2016_AUST" t2
ON ST_CONTAINS(ST_SETSRID(t2.geometry, 4326), ST_SetSRID(ST_POINT(t1.longitude, t1.latitude), 4326))
;
"""
task_extract_airbnb_into_fact = PostgresOperator(
    task_id='extract_airbnb_into_fact',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#   Create dim table host
#########################################################
query = """
CREATE TABLE IF NOT EXISTS star.dim_host (
    host_id BIGINT PRIMARY KEY, 
	host_url TEXT, 
	host_name TEXT, 
	host_since TEXT, 
	host_location TEXT, 
	host_about TEXT, 
	host_response_time TEXT, 
	host_response_rate TEXT, 
	host_acceptance_rate TEXT, 
	host_is_superhost BOOLEAN, 
	host_thumbnail_url TEXT, 
	host_picture_url TEXT, 
	host_neighbourhood TEXT, 
	host_listings_count FLOAT(53), 
	host_total_listings_count FLOAT(53), 
	host_verifications TEXT, 
	host_has_profile_pic TEXT, 
	host_identity_verified TEXT 
)
"""
task_create_host_table = PostgresOperator(
    task_id='create_host_table',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#   Extract host from listing into dim
#########################################################
query = """
INSERT INTO star.dim_host
SELECT
  t1.host_id
, MAX(t1.host_url) AS host_url
, MAX(t1.host_name) AS host_name
, MAX(t1.host_since) AS host_since
, MAX(t1.host_location) AS host_location
, MAX(t1.host_about) AS host_about
, MAX(t1.host_response_time) AS host_response_time
, MAX(t1.host_response_rate) AS host_response_rate
, MAX(t1.host_acceptance_rate) AS host_acceptance_rate
, MAX(t1.host_is_superhost)::BOOLEAN AS host_is_superhost
, MAX(t1.host_thumbnail_url) AS host_thumbnail_url
, MAX(t1.host_picture_url) AS host_picture_url
, MAX(t1.host_neighbourhood) AS host_neighbourhood
, MAX(t1.host_listings_count) AS host_listings_count
, MAX(t1.host_total_listings_count) AS host_total_listings_count
, MAX(t1.host_verifications) AS host_verifications
, MAX(t1.host_has_profile_pic) AS host_has_profile_pic
, MAX(t1.host_identity_verified) AS host_identity_verified
FROM raw.airbnb_listing_latest t1
WHERE t1.host_id NOT IN (
  SELECT DISTINCT(host_id)
  FROM star.dim_host
) 
GROUP BY 1;

"""
task_extract_host_into_dim = PostgresOperator(
    task_id='extract_host_into_dim',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#   Create property table
#########################################################
query = """
CREATE TABLE IF NOT EXISTS star.dim_property (
    id BIGINT PRIMARY KEY, 
--	listing_url TEXT, 
 	name TEXT, 
    description TEXT, 
	neighborhood_overview TEXT, 
	picture_url TEXT, 
    neighbourhood_cleansed TEXT, 
    latitude FLOAT(53), 
	longitude FLOAT(53), 
	property_type TEXT, 
	room_type TEXT, 
	accommodates BIGINT, 
--	bathrooms FLOAT(53), 
--	bedrooms FLOAT(53), 
	--beds FLOAT(53), 
	--amenities TEXT, 
	price FLOAT(53), 
	number_of_reviews BIGINT, 
	license FLOAT(53), 
	instant_bookable BOOLEAN
)
"""
task_create_property_table = PostgresOperator(
    task_id='create_property_table',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#   Extract property from listing into dim
#########################################################
query = """
INSERT INTO star.dim_property
SELECT
    id,
--    listing_url,
    name,
    description,
    neighborhood_overview,
    picture_url,
    neighbourhood_cleansed,
    latitude::FLOAT(53),
    longitude::FLOAT(53),
    property_type::TEXT,
    room_type::TEXT,
    accommodates::BIGINT,
--    bedrooms::FLOAT(53),
--    beds,
--    amenities,
    price::FLOAT(53),
    number_of_reviews::BIGINT,
    license::FLOAT(53),
    instant_bookable::BOOLEAN
FROM raw.airbnb_listing_latest t1
WHERE t1.id NOT IN (
  SELECT DISTINCT(id)
  FROM star.dim_property
) 
;
"""
task_extract_property_into_dim = PostgresOperator(
    task_id='extract_property_into_dim',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag
)

#########################################################
#   DAG organisation
#########################################################
task_create_history_table >> task_load_listing_from_file
[task_load_listing_from_file,
 task_dict_create_schemas['raw']] >> task_upload_listing_to_postgres
task_dict_create_schemas['star'] >> list(task_dict_load_from_file_upload_to_postgres.values())
task_dict_create_schemas['star'] >> [
    task_create_fact_airbnb_table,
    task_create_host_table,
    task_create_property_table
    ]
task_dict_load_from_file_upload_to_postgres['LGA_2016_AUST'] >> task_extract_airbnb_into_fact
[task_create_property_table,
 task_extract_airbnb_into_fact] >> task_extract_property_into_dim
task_dict_create_schemas['meta'] >> task_create_history_table
[task_create_host_table,
 task_extract_airbnb_into_fact] >> task_extract_host_into_dim
[task_create_fact_airbnb_table,
 task_upload_listing_to_postgres] >> task_extract_airbnb_into_fact

#########################################################
#########################################################
#   Populating the datamart
#########################################################
#########################################################

#########################################################
#   Task: create the data_mart.table{1, 2, 3}
#########################################################
