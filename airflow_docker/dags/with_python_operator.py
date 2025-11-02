from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

def get_name():
    return "Airflow"

def print_hello(age, ti):
    name = ti.xcom_pull(task_ids='get_name')
    print(f"Hello world from PythonOperator name: {name}, age: {age}!")

with DAG(dag_id="first_python_dag_v3", 
         default_args=default_args,
         start_date=datetime(2025, 10, 12, 1,1,1),
         schedule="@daily", 
         catchup=False) as dag:
        task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
        )
        task1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
        op_kwargs={'age': 5}
        )      


        task2 >> task1