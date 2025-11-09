from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id="first_taskflow_dag_v6",
     default_args=default_args,
     start_date=datetime(2025, 10, 12, 1,1,1),
     schedule="@daily")

def taskflow_api_etl():
    @task
    def get_age():
        return 35

    @task
    def get_name():
        firstname = 'Airflow'
        lastname = 'User'
        return firstname, lastname

    @task
    def print_hello(name_data, age):
        firstname, lastname = name_data
        print(f"Hello world from TaskFlow API name: {firstname} {lastname}, age: {age}!")

    name_data = get_name()
    age = get_age()
    print_hello(name_data, age)

dag = taskflow_api_etl()