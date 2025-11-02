from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id="first_dag_v2", 
         default_args=default_args,
         start_date=datetime(2025, 10, 12, 1,1,1),
         schedule="@daily", 
         catchup=False) as dag:
    task1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )
    task2 = BashOperator(
        task_id='Echo_world',
        bash_command='echo "Hello world_2"'
    )

    task1.set_downstream(task2)