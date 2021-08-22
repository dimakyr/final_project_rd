from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from get_data_from_api import write_data_to_bronze, bronze_to_silver


today = datetime.today().strftime('%Y-%m-%d')

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 1
}

dag = DAG(
    dag_id='final_project_api',
    description='Final project',
    schedule_interval='@daily',
    start_date=datetime(2021, 8, 10, 23),
    default_args=default_args
)

get_data_to_bronze = PythonOperator(
    task_id=f'get_data_from_api_by_day_to_bronze',
    python_callable=write_data_to_bronze,
    op_kwargs={'current_date': today},
    dag=dag
)

transform_data_to_silver = PythonOperator(
    task_id=f'transform_data_to_silver',
    python_callable=bronze_to_silver,
    op_kwargs={'current_date': today},
    dag=dag
)

get_data_to_bronze >> transform_data_to_silver

