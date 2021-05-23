import pandas as pd
from typing import (
    Optional,
    List
)
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
references_dir = project_dir / 'references'
conn_string = get_connection_string()
engine = sa.create_engine(conn_string)
postgres_conn_id = 'postgres_airflow'

#########################################################
#   Set up DAG
#########################################################

dag_default_args = {
    'owner': 'roger_yu',
    'start_date': datetime.now() - relativedelta(months=13),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='calculate_airbnb_kpis_2',
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
#   Convenience functions
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
    engine = sa.create_engine(get_connection_string())
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
    engine = sa.create_engine(get_connection_string())
    df = pd.DataFrame({'table_name': [table_name],
                       'year_scraped': year_scraped,
                       'month_scraped': month_scraped,
                       'n_rows': df.shape[0],
                       'n_cols': df.shape[1]})
    df.to_sql(con=engine,
              name='history',
              schema='meta',
              if_exists='append',
              index=False
    )

#########################################################
#   Task: Upload suburb mapping
#########################################################
def _upload_mapping() -> None:
    """
    Uploads the mapping file to Postgres
    :return:
    """
    table_name = 'host_neighbourhood_mapping'
    engine = sa.create_engine(get_connection_string())
    if not is_loaded(table_name=table_name, engine=engine):
        path = references_dir / 'host_neighbourhood_mapping.csv'
        conn_string = get_connection_string()
        df = pd.read_csv(path)
        df.to_sql(con=conn_string,
                  schema='star',
                  name='host_neighbourhood_mapping',
                  index=False,
                  if_exists='replace')

        update_history(df=df,
                       year_scraped=0,
                       month_scraped=0,
                       table_name=table_name,
                       engine=engine)

task_upload_mapping = PythonOperator(
    task_id='upload_mapping',
    python_callable=_upload_mapping,
    dag=dag
)

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
            path = next(dir.glob(f'{year}-{str(month).zfill(2)}*.gz'))
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
    engine = sa.create_engine(get_connection_string())
    if df is not None:
        execution_date = kwargs['execution_date'].strftime('%Y%m')
        table_name = f'airbnb_listing_{execution_date}'

        # Connect to Postgres
        conn_string = get_connection_string(db_name='airflow')
        logging.info(f'conn_string: {conn_string}')
        engine = sa.create_engine(conn_string)
        logging.info(f'df.execution_date.loc[0]: {df.execution_date.loc[0]}')
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

        update_history(df=df,
                       year_scraped=int(year),
                       month_scraped=int(month),
                       table_name=table_name,
                       engine=engine)


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
    if not is_loaded(table_name=table_name, engine=engine):
        path = dir / filename
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
            )

            if 'LGA' in table_name:
                # For consistency with the census tables
                df.rename(columns={'LGA_CODE16': 'LGA_CODE_2016'})

            df.to_postgis(con=engine,
                          name=table_name,
                          schema=schema,
                          if_exists='replace',
                          index=False)
        update_history(df=df, year_scraped=0, month_scraped=0,
                       table_name=table_name, engine=engine)

paths = [
    raw_data_dir / '2016Census_G01_NSW_LGA.csv',
    raw_data_dir / '2016Census_G02_NSW_LGA.csv',
    raw_data_dir / 'shapefile/LGA_2016_AUST.shp',
    raw_data_dir / 'shapefile_ssc_2011/SSC_2011_AUST.shp'
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
query = """
CREATE TABLE IF NOT EXISTS star.fact_airbnb (
	id BIGINT,
	host_id BIGINT, 
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
--	license FLOAT(53), 
	instant_bookable BOOLEAN, 
	calculated_host_listings_count BIGINT, 
	calculated_host_listings_count_entire_homes BIGINT, 
	calculated_host_listings_count_private_rooms BIGINT, 
	calculated_host_listings_count_shared_rooms BIGINT, 
	reviews_per_month FLOAT(53),
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
    t1.host_id,
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
--	t1.license, 
	t1.instant_bookable::BOOLEAN, 
	t1.calculated_host_listings_count, 
	t1.calculated_host_listings_count_entire_homes, 
	t1.calculated_host_listings_count_private_rooms, 
	t1.calculated_host_listings_count_shared_rooms, 
	t1.reviews_per_month
FROM raw.airbnb_listing_latest t1
--LEFT JOIN star."LGA_2016_AUST" t2
--ON ST_CONTAINS(ST_SETSRID(t2.geometry, 4326), ST_SetSRID(ST_POINT(t1.longitude, t1.latitude), 4326))
ON CONFLICT DO NOTHING;
"""
task_extract_airbnb_into_fact = PostgresOperator(
    task_id='extract_airbnb_into_fact',
    postgres_conn_id='postgres_airflow',
    sql=query,
    dag=dag,
    execution_timeout=timedelta(seconds=1000),
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
GROUP BY 1
ON CONFLICT DO NOTHING;
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
    host_id BIGINT,
    "LGA_CODE_2016" TEXT,
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
--	license FLOAT(53), 
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
    t1.id,
    t1.host_id,
    t2."LGA_CODE16",
--    t1.listing_url,
    t1.name,
    t1.description,
    t1.neighborhood_overview,
    t1.picture_url,
    t1.neighbourhood_cleansed,
    t1.latitude::FLOAT(53),
    t1.longitude::FLOAT(53),
    t1.property_type::TEXT,
    t1.room_type::TEXT,
    t1.accommodates::BIGINT,
--    t1.bedrooms::FLOAT(53),
--    t1.beds,
--    t1.amenities,
    t1.price::FLOAT(53),
    t1.number_of_reviews::BIGINT,
--    t1.license::FLOAT(53),
    t1.instant_bookable::BOOLEAN
FROM raw.airbnb_listing_latest t1
LEFT JOIN star."LGA_2016_AUST" t2
ON ST_CONTAINS(ST_SETSRID(t2.geometry, 4326), ST_SetSRID(ST_POINT(t1.longitude, t1.latitude), 4326))
-- Select only those not already in the target table
WHERE t1.id NOT IN (
  SELECT DISTINCT(id)
  FROM star.dim_property
) 

ON CONFLICT DO NOTHING
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
task_create_history_table >> list(task_dict_load_from_file_upload_to_postgres.values())
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
query = """
CREATE TABLE IF NOT EXISTS data_mart.neighbourhood_month_year (
    year INT, 
    month INT, 
 	neighbourhood_cleansed TEXT,
 	active_listing_rate FLOAT,
    min_price FLOAT,
    max_price FLOAT,
    median_price FLOAT,
    average_price FLOAT,
    superhost_rate FLOAT,
    average_review_scores FLOAT,
    pct_change_active_listings FLOAT,
    pct_change_inactive_listings FLOAT,
    n_stays FLOAT,
    est_revenue FLOAT
)
"""

def _calculate_kpis(engine: sa.engine.base.Engine,
                    groupby: List[str],
                    table_name: str,
                    **kwargs) -> None:
    """
    Calculate the required KPIs and upload to `data_mart.table1`. The
    calculation is to be done on a per month basis, because the data is loaded
    monthly.
    :param engine:
    :param groupby: A list of items to groupby
    :param table_name: The name of the table to be uploaded to in `data_mart`
    :param kwargs:
    :return:
    """
    ds = kwargs['ds']
    prev_ds = kwargs['prev_ds']
    year, month, _ = ds.split('-')
    year_previous, month_previous, _ = kwargs['prev_ds'].split('-')

    # Load initial tables
    query = f"""
    SELECT * 
    FROM star.fact_airbnb
    WHERE execution_date = '{ds}'
    """
    query_prev = f"""
        SELECT * 
        FROM data_mart.{table_name}
        WHERE execution_date = '{prev_ds}'
        """

    df = pd.read_sql(con=engine, sql=query)
    logging.info(f'execution_date: {ds}')
    logging.info(f'df.columns: {df.columns}')
    try:
        df_results_prev = pd.read_sql(con=engine, sql=query_prev)
    except:
        df_results_prev = None
    df_host = pd.read_sql(con=engine, sql='SELECT * FROM star.dim_host')
    df_property = pd.read_sql(con=engine, sql='SELECT * FROM star.dim_property')
    df_property.drop(columns=['host_id'], inplace=True)
    gdf_lga = gpd.GeoDataFrame.from_postgis(
        con=engine,
        sql='SELECT * FROM star."LGA_2016_AUST"',
        geom_col='geometry'
    )

    # Merge tables
    df_merged = (
        df
        .merge(df_host, on='host_id', how='left')
        .merge(df_property, on='id', how='left')
    )
    gdf = gpd.GeoDataFrame(
        df_merged,
        geometry=gpd.points_from_xy(df_merged.longitude, df_merged.latitude)
    )
    gdf_join_lga = gpd.sjoin(left_df=gdf.set_crs('EPSG:4283'),
                             right_df=gdf_lga,
                             op='intersects',
                             how='left')
    logging.info(f'gdf_join_lga.columns: {gdf_join_lga.columns}')
    logging.info(f'groupby: {groupby}')
    df_grouped = gdf_join_lga.groupby(groupby)
    logging.info(f'df_grouped.size().max(): {df_grouped.size().max()}')

    def agg_group(df: pd.DataFrame) -> pd.Series:
        """
        To be used in pandas.core.groupby.GroupBy.apply
        :param df: Grouped dataframes
        :return: A pd.Series of a collection of pd.Series
        """
        calc_list = []
        calc_names = [
            'active_listing_rate',
            'average_review_scores_rating',
            'min_price',
            'max_price,',
            'median_price',
            'average_price',
            'n_distinct_hosts',
            'superhost_rate',
            'n_stays',
            'est_revenue_per_active_listing',
            'n_active_listings',
            'n_inactive_listings',
        ]
        calc_list.append(df.has_availability.mean() * 100)
        calc_list.append(df.query('has_availability').review_scores_rating.mean())
        calc_list.append(df.price.min())
        calc_list.append(df.price.max())
        calc_list.append(df.price.median())
        calc_list.append(df.price.mean())
        calc_list.append(df.host_id.nunique())
        calc_list.append(df.drop_duplicates(subset=['host_id']).host_is_superhost.mean())
        calc_list.append((30 - df.query('has_availability').availability_30).sum())
        calc_list.append(((30 - df.query('has_availability').availability_30) * df.query(
                'has_availability').price).mean())
        calc_list.append(df.has_availability.sum())
        calc_list.append((~df.has_availability).sum())

        result = pd.Series(calc_list, index=calc_names)
        return result

    result_df = df_grouped.apply(agg_group).reset_index()
    logging.info(f'result_df.shape: {result_df.shape}')
    # Calculate percentage change
    # There is no percentage change for the first month
    if df_results_prev is None:
        result_df.loc[:, 'n_active_listings_pct_change'] = np.nan
        result_df.loc[:, 'n_inactive_listings_pct_change'] = np.nan
    else:
        # Change the column names of the previous values
        current_listing_cols = ['n_active_listings', 'n_inactive_listings']
        prev_listing_cols = ['n_active_listings_prev', 'n_inactive_listings_prev']
        rename_dict = dict(zip(current_listing_cols, prev_listing_cols))

        def calc_perc(df, col, col_prev):
            return (df[col] - df[col_prev]) / df[col_prev] * 100

        prev_cols =  groupby + list(rename_dict.keys())
        logging.info(f'df_results_prev.columns: {df_results_prev.columns}')
        logging.info(
            f'df_results_prev[prev_cols].columns: {df_results_prev[prev_cols].columns}')
        result_df = (
            result_df
            .merge(df_results_prev[prev_cols]
                   .rename(columns=rename_dict),
                   on=groupby,
                   how='left')
            .assign(n_active_listings_pct_change = lambda x: calc_perc(
                x,
                current_listing_cols[0],
                prev_listing_cols[0]),
                    n_inactive_listings_pct_change=lambda x: calc_perc(
                        x,
                        current_listing_cols[1],
                        prev_listing_cols[1]),
                    )
            # Don't need the previous values anymore
            .drop(columns=rename_dict.values())
        )
    result_df.loc[:, 'execution_date'] = datetime(int(year), int(month), 1)
    # Adding the LGA names for `data_mart.table`
    if table_name == 'table1':
        result_df = result_df.merge(gdf_lga[['LGA_CODE16', 'LGA_NAME16']],
                                    on='LGA_CODE16',
                                    how='left')
    result_df.to_sql(con=engine,
                     schema='data_mart',
                     name=table_name,
                     if_exists='append',
                     index=False)

def _join_ssc(df: pd.DataFrame,
              engine: sa.engine.base.Engine) -> None:
    """
    Join suburb level data, from `star.SSC_2011_AUST` to `table_name`. The goal
    is to produce `host_neighbourhood_cleansed`.
    :param df: The dataframe to be added the suburb `geometry`.
    :param engine:
    :param kwargs:
    :return:
    """

    ssc_df = gpd.GeoDataFrame.from_postgis(
        con=engine,
        sql='SELECT * FROM star."SSC_2011_AUST"',
        geom_col='geometry'
    )
    logging.info(f'ssc_df.shape: {ssc_df.shape}')

    # The values of `host_neighbourhood` don't all match to the SSC_NAME of
    # the suburb data
    df_mapping = pd.read_csv(references_dir / 'host_neighbourhood_mapping.csv',
                             encoding='latin')
    logging.info(f'df_mapping.shape: {df_mapping.shape}')
    replace_dict = dict(zip(df_mapping.host_neighbourhood, df_mapping.suburb))
    df.loc[:, 'host_neighbourhood_cleansed'] = (
        df.host_neighbourhood
        .replace(replace_dict)
        # For values with a forward slash, take only the first value.
        .str.split('/')
        .str[0]
        .str.strip()
    )

    ssc_df_clean = (
        ssc_df
        # Some suburb names are duplicated in other states.
        .assign(in_nsw = lambda x: x.SSC_NAME.str.contains('NSW'),
                is_duplicated = lambda x: x.SSC_NAME.str.contains('\('),
                SSC_NAME_cleaned = lambda x: x.SSC_NAME.str.split('(').str[0].str.strip())
        .dropna(subset=['geometry'])
    )

    # Join the geometry of the suburb.
    drop_index = ssc_df_clean.loc[lambda x: ~x.in_nsw & x.is_duplicated].index
    ssc_df_clean.drop(drop_index, inplace=True)

    df_merged = (
        ssc_df
        .merge(df,
               left_on='SSC_NAME',
               right_on='host_neighbourhood_cleansed',
               how='right')
    )

    return df_merged


def _calculate_kpis_3(engine: sa.engine.base.Engine,
                      groupby: List[str],
                      table_name: str,
                      **kwargs) -> None:
    ds = kwargs['ds']
    prev_ds = kwargs['prev_ds']
    year, month, _ = ds.split('-')

    query = f"""
        SELECT * 
        FROM star.fact_airbnb
        WHERE execution_date = '{ds}'
        """
    # Load the initial dataframes
    df = pd.read_sql(con=engine, sql=query)
    df_host = pd.read_sql(con=engine, sql='SELECT * FROM star.dim_host')
    df_property = pd.read_sql(con=engine,
                              sql='SELECT * FROM star.dim_property')
    df_property.drop(columns=['host_id'], inplace=True)
    df_lga = gpd.GeoDataFrame.from_postgis(
        sql='SELECT * FROM star."LGA_2016_AUST"',
        con=engine,
        geom_col='geometry'
    )
    # Merge dataframes
    df_merged_host = df.merge(df_host, on='host_id', how='left')

    df_ssc = _join_ssc(df_merged_host, engine)
    df_merged_lga = gpd.sjoin(
        left_df=df_ssc,
        right_df=df_lga,
        op='intersects',
        how='left')
    df_merged = (
        df_merged_lga
            .merge(df_host, on='host_id', how='left')
            .merge(df_property, on='id', how='left')
    )

    def agg_group(df: pd.DataFrame) -> pd.Series:
        """
        To be used in pandas.core.groupby.GroupBy.apply
        :param df: Grouped dataframes
        :return: A pd.Series of a collection of pd.Series
        """
        calc_list = []
        calc_names = [
            'n_distinct_hosts',
            'est_revenue',
            'est_revenue_per_host'
        ]

        calc_list.append(df.host_id.nunique())
        calc_list.append(((30 - df.availability_30) * df.price).sum())
        calc_list.append(
            ((30 - df.availability_30) * df.price).sum() / df.host_id.nunique()
        )

        result = pd.Series(calc_list, index=calc_names)
        return result

    result_df = (
        df_merged
        .groupby(groupby)
        .apply(agg_group)
    )
    result_df.loc[:, 'execution_date'] = datetime(int(year),  int(month), 1)
    result_df.to_sql(con=engine,
                     schema='data_mart',
                     name=table_name,
                     if_exists='append')

task_calculate_kpis_table1 = PythonOperator(
    task_id='calculate_kpis_table1',
    python_callable=_calculate_kpis,
    op_kwargs={'engine': engine,
               'groupby': ['LGA_CODE16'],
               'table_name': 'table1'},
    execution_timeout=timedelta(seconds=1000),
    dag=dag
)

task_calculate_kpis_table2 = PythonOperator(
    task_id='calculate_kpis_table2',
    python_callable=_calculate_kpis,
    op_kwargs={'engine': engine,
               'groupby': ['property_type', 'room_type', 'accommodates'],
               'table_name': 'table2'},
    execution_timeout=timedelta(seconds=1000),
    dag=dag
)

task_calculate_kpis_table3 = PythonOperator(
    task_id='calculate_kpis_table3',
    python_callable=_calculate_kpis_3,
    op_kwargs={'engine': engine,
               'groupby': ['LGA_NAME16'],
               'table_name': 'table3'},
    execution_timeout=timedelta(seconds=1000),
    dag=dag
)

task_extract_host_into_dim >> [task_calculate_kpis_table1,
                               task_calculate_kpis_table2,
                               task_calculate_kpis_table3]
task_extract_property_into_dim >> [task_calculate_kpis_table1,
                                   task_calculate_kpis_table2,
                                   task_calculate_kpis_table3]

