 
import pandas as pd
import re
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import time
from datetime import datetime
from datetime import date
import os
import configparser
import numpy as np

def get_server_info(sname):
    if '__file__' in globals():
        workpath = os.path.dirname(os.path.abspath(__file__))
    else:
        workpath = os.getcwd() #for .ipynb files
    
    config = configparser.ConfigParser()
    config.read(os.path.join(workpath, 'server.ini'))
    if config.has_option(sname, 'server') and config.get(sname, 'server') != '':
        server = config.get(sname, 'server')
        database = config.get(sname, 'database')
        username = config.get(sname, 'username')
        password = config.get(sname, 'password')
        if  config.has_option(sname, 'driver'):
            driver = config.get(sname, 'driver')
        else:
            driver = ''
    else:
        pass
    return server,database,username,password,driver

def read_from_sql_multi(sql_query, con_name, params=None):
    server, database, username, password, driver = get_server_info(con_name)
    connection_string = (
    f'mssql+pyodbc://{username}:{password}@{server}/{database}'
    f'?driver=ODBC+Driver+18+for+SQL+Server'
    f'&TrustServerCertificate=yes'
)
    engine = create_engine(connection_string)
    try:
        with engine.connect() as connection:
            result = pd.read_sql_query(sql_query, connection, params=params)
            return result
    finally:
        engine.dispose()


def pyodbc_write_df_to_sql(
    df,
    conn,
    table_name,
    query=None,
    chunk_size=10000,
    if_exists='append'  # options: 'append', 'replace'
):
    """
    Write a DataFrame to SQL Server using pyodbc. Automatically creates the table if it doesn't exist.
    Filters DataFrame columns based on those listed in the provided query (if any).
    Parameters:
    df (pd.DataFrame): DataFrame to insert into the database.
    conn (pyodbc.Connection): An active pyodbc connection object.
    table_name (str): Name of the target table.
    query (str, optional): SQL 'INSERT INTO TableName (col1, col2, ...)' statement (without VALUES). If None, it's generated from df.
    chunk_size (int): Number of rows per batch insert (default: 10,000).
    if_exists (str): 'append' (default) to add to an existing table, 'replace' to drop and recreate the table.
    Returns:
    None
    """
    cursor = conn.cursor()
    try:
        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ?
        """, (table_name,))
        exists = cursor.fetchone()[0]
        # Drop table if 'replace'
        if if_exists == 'replace' and exists:
            cursor.execute(f"DROP TABLE {table_name}")
            conn.commit()
            exists = 0
        # Create table if it doesn't exist
        if not exists:
            col_defs = []
            for col in df.columns:
                dtype = df[col].dtype
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "DECIMAL(38, 10)"  # En geniş sayı aralığı
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = "DATETIME"
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = "BIT"
                else:
                    sql_type = "VARCHAR(255)"
                col_defs.append(f"[{col}] {sql_type}")
            create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
            cursor.execute(create_sql)
            conn.commit()
        # Query sütun eşlemesi
        if query:
            column_match = re.search(r"\((.*?)\)", query, re.DOTALL)
            if column_match:
                column_str = column_match.group(1)
                selected_columns = [col.strip().strip("[]") for col in column_str.split(",")]
                df = df[selected_columns]
            else:
                raise ValueError("Invalid query format.")
        else:
            col_names = ', '.join(f"[{col}]" for col in df.columns)
            query = f"INSERT INTO {table_name} ({col_names})"
        # Temizlik
        df = df.replace([np.inf, -np.inf, '', ' ', 'NaN', 'nan', np.nan], 0)
        # Hazırlık
        placeholders = ', '.join(['?'] * df.shape[1])
        full_query = f"{query} VALUES ({placeholders})"
        data = list(df.itertuples(index=False, name=None))
        # Veriyi gönder
        total_rows = len(data)
        cursor.fast_executemany = True
        for i in range(0, total_rows, chunk_size):
            chunk = data[i:i + chunk_size]
            cursor.executemany(full_query, chunk)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"An error occurred while writing data: {e}")
    finally:
        cursor.close()
        conn.close() 
 