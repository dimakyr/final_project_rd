import sys

from hdfs import InsecureClient
import psycopg2
import os
import logging
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession


def get_airflow_connections(connection_id):
    connection = BaseHook.get_connection(connection_id)
    logging.info(
        f'Connection config: {connection.host}, DB {connection.schema}, Login {connection.login}, PWD {connection.password}')
    creds = {
        'host': connection.host,
        'port': connection.port,
        'database': connection.schema,
        'user': connection.login,
        'password': connection.password
    }
    return creds


def get_table_data_for_file(**kwargs):
    client = InsecureClient('http://127.0.0.1:50070/', user='user')
    creds = get_airflow_connections(kwargs['connection_id'])
    try:
        connect = psycopg2.connect(**creds)
        cursor = connect.cursor()
        table_name = kwargs['table']
        today = kwargs['current_date']
        logging.info(f'Table: {table_name}')
        with client.write(
                os.path.join('bronze', today, kwargs['connection_id'],  f'{table_name}.csv'), overwrite=True
        ) as csv:
            cursor.copy_expert(f'COPY {table_name} to STDOUT WITH HEADER CSV', csv)
        logging.info(f"File: {os.path.join('bronze', today, kwargs['connection_id'], f'{table_name}.csv')}")
        cursor.close()
        logging.info('*****success********')
    except psycopg2.Error as e:
        logging.error(e)
        sys.exit(1)


def bronze_to_silver(**kwargs):
    try:
        table_name = kwargs['table']
        today = kwargs['current_date']
        spark = SparkSession.builder.master('local').appName('final').getOrCreate()

        table_data_df = spark.read.load(os.path.join('bronze', today, kwargs['connection_id'], f'{table_name}.csv'),
                                        header="true",
                                        inferSchema="true",
                                        format="csv")
        table_data_df = table_data_df.dropDuplicates()
        logging.info(f'{table_name}: duplicates delete')
        table_data_df.write.parquet(os.path.join('silver', today, kwargs['connection_id'], table_name),
                                    mode='overwrite')
        logging.info(f'{table_name}.parquet successfully saved to SILVER')
    except:
        logging.error(f'There are problems with data processing, please see the execution logs')
        sys.exit(1)
