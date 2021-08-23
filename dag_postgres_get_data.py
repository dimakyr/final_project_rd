from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from get_data_from_db import get_table_data_for_file, bronze_to_silver
from get_data_from_api import write_data_to_bronze, bronze_to_silver_from_api


today = datetime.today().strftime('%Y-%m-%d')


def get_tables_from_variables(key):
    tables = Variable.get(key=key, deserialize_json=True)
    return tables['dshop_bu']


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 1
}

dag = DAG(
    dag_id='final_project_postgres',
    description='Final project',
    schedule_interval='@daily',
    start_date=datetime(2021, 8, 10, 23),
    default_args=default_args
)

get_data_to_bronze = []
transform_data_to_silver = []

get_data_to_bronze_from_api = PythonOperator(
    task_id=f'get_data_from_api_by_day_to_bronze',
    python_callable=write_data_to_bronze,
    op_kwargs={'current_date': today},
    dag=dag
)

transform_data_to_silver_from_api = PythonOperator(
    task_id=f'transform_data_to_silver',
    python_callable=bronze_to_silver_from_api,
    op_kwargs={'current_date': today},
    dag=dag
)


for table in get_tables_from_variables('tables'):
    get_data_to_bronze.append(PythonOperator(
        task_id=f'get_{table}_from_postgres',
        python_callable=get_table_data_for_file,
        op_kwargs={
            'connection_id': 'dshop_bu',
            'table': table,
            'current_date': today
        },
        dag=dag
    ))

    transform_data_to_silver.append(PythonOperator(
        task_id=f'push_{table}_to_silver',
        python_callable=bronze_to_silver,
        op_kwargs={
            'connection_id': 'dshop_bu',
            'table': table,
            'current_date': today
        },
        dag=dag
    ))

dummy_task1 = DummyOperator(task_id='dummy_task1', dag=dag)
dummy_task2 = DummyOperator(task_id='dummy_task2', dag=dag)

get_data_to_bronze_from_api >> transform_data_to_silver_from_api >>\
dummy_task1 >> get_data_to_bronze >> dummy_task2 >> transform_data_to_silver