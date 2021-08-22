import sys
import logging
import requests
import os
import json

from airflow.models import Variable
from hdfs import InsecureClient
from pyspark.sql import SparkSession


def get_jwt_token():
    api = Variable.get(key='api', deserialize_json=True)
    auth = Variable.get(key='auth', deserialize_json=True)
    url = f'{api["url"]}{auth["endpoint"]}'
    headers = {
        'Content-Type': 'application/vnd.api+json'
    }
    response = requests.post(url, headers=headers, data=json.dumps(auth['payload']))
    response.raise_for_status()
    return response.json()['access_token']


def get_data_from_server(date):
    api = Variable.get(key='api', deserialize_json=True)
    data = {}
    token = get_jwt_token()
    url = f"{api['url']}{api['endpoint']}?date={date}"
    headers = {
        'Authorization': f'JWT {token}'
    }
    result = requests.get(url, headers=headers, timeout=1)
    if 'message' in result.json() and result.json()['message'] == 'No out_of_stock items for this date':
        logging.error('No out_of_stock items for this date')
        sys.exit(1)
    data[date] = result.json()
    return data


def write_data_to_bronze(**kwargs):
    today = kwargs['current_date']
    data = get_data_from_server(today)
    client = InsecureClient('http://127.0.0.1:50070/', user='user')
    logging.info(f'Create file: bronze/{today}/out_of_stock_api/{today}.json')
    with client.write(os.path.join('bronze', today, 'out_of_stock_api', f'{today}.json'), encoding='utf-8',
                      overwrite=True) as f:
        json.dump(data[today], f)


def bronze_to_silver(**kwargs):
    try:
        today = kwargs['current_date']
        spark = SparkSession.builder.master('local').appName('hw_14').getOrCreate()

        table_data_df = spark.read.load(os.path.join('bronze', today, 'out_of_stock_api', f'{today}.json'),
                                        header="true",
                                        inferSchema="true",
                                        format="json")
        table_data_df = table_data_df.dropDuplicates()
        logging.info(f'{today}.json: duplicates delete')
        table_data_df.write.parquet(os.path.join('silver', today, 'out_of_stock_api', today),
                                    mode='overwrite')
        logging.info(f'{today}.parquet successfully saved to SILVER')
    except:
        logging.error(f'There are problems with data processing, please see the execution logs')
        sys.exit(1)
