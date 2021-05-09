import sqlalchemy as sa
from dotenv import (
    find_dotenv,
    load_dotenv
)
import os


def get_connection_string(db_type: str='postgres',
                          db_name: str='airflow') -> str:
    """

    :return: a connection string
    """
    load_dotenv(find_dotenv())
    POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
    POSTGRES_PORT = os.environ.get('POSTGRES_PORT')
    POSTGRES_USERNAME = os.environ.get('POSTGRES_USERNAME')
    POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
    POSTGRES_DB = os.environ.get('POSTGRES_DB')

    conn = f'postgresql+psycopg2://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@' \
           f'{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}'

    return conn
