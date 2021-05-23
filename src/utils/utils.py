import pandas as pd
import numpy as np


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

def get_create_query(table_name: str,
                     fields: pd.Series,
                     dtypes: pd.Series,
                     keys: pd.Series) -> str:
    """
    Creates a SQL query to create a table
    :param table_name:
    :param fields:
    :param dtypes:
    :param keys:
    :return:
    """
    stringified_columns = stringify_columns(fields,
                                            dtypes,
                                            keys,
                                            with_dtype=True)
    query = f'CREATE TABLE {table_name}(\n{stringified_columns}\n)'
    return query
