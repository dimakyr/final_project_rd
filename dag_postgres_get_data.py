from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from get_data_from_db import get_table_data_for_file, bronze_to_silver


today = datetime.today().strftime('%Y-%m-%d')


def get_tables_from_variables(key):
    tables = Variable.get(key=key, deserialize_json=True)
    return tables['dshop']


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 1
}

dag = DAG(
    dag_id='hw_14_postgres',
    description='Home work for lesson 14',
    schedule_interval='@daily',
    start_date=datetime(2021, 8, 10, 23),
    default_args=default_args
)
