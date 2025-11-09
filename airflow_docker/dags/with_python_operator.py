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
def get_age(ti):
     ti.xcom_push(key='age', value=35)

def get_name(ti):
    ti.xcom_push(key='firstname', value='Airflow')
    ti.xcom_push(key='lastname', value='User')

def print_hello(ti):
    firstname = ti.xcom_pull(task_ids='get_name', key='firstname')
    lastname = ti.xcom_pull(task_ids='get_name', key='lastname')
    age = ti.xcom_pull(task_ids='age_task', key='age')
    print(f"Hello world from PythonOperator name: {firstname} {lastname}, age: {age}!")

with DAG(dag_id="first_python_dag_v5", 
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
            python_callable=print_hello
            #op_kwargs={'age': 5}
        )      

        task3 = PythonOperator(
            task_id='age_task',
            python_callable=get_age
        )


        [task2, task3] >> task1