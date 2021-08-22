from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from get_data_from_db import get_table_data_for_file, bronze_to_silver


today = datetime.today().strftime('%Y-%m-%d')


def get_tables_from_variables(key):
    tables = Variable.get(key=key, deserialize_json=True)
    return tables['dshop']


