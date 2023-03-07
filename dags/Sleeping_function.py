from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
def hello_function():
    print("Hello, this is the first task done by sudip")
    time.sleep(5)
def last_function():
    print("The task run has been completed")
def sleeping_function():
    print("This is the sleeping function for 5 seconds\n")
    print("Sleeping for 5 seconds, dont mind!")

with DAG(
    dag_id="Local_execution_demo",
    start_date=datetime(2023,3,7),
    schedule_interval="@hourly",
    catchup=False
) as dag:
    task1=PythonOperator(
    task_id="hello_function",
    python_callable=hello_function
    )
    task2_1=PythonOperator(
    task_id="sleepy_1",
    python_callable=sleeping_function
    )
    task2_2=PythonOperator(
    task_id="sleepy_2",
    python_callable=sleeping_function
    )
    task3=PythonOperator(
    task_id="bye_function",
    python_callable=last_function
    )


task1>>[task2_1,task2_2] >> task3
                