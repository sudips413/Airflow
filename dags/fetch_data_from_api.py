import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
def fetch_from_api():
    try:
        url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
        response = requests.request("GET", url)
        print("------------------------")
        print(response.text)
        print("-----------------------")
        with open("/home/sudip/airflow/data/response.csv", "w") as file:
            file.write(response.text)
    except:
        print("Error while fetching!!!!!!!!")        
def preprocess():
    try:
        df=pd.read_csv("/home/sudip/airflow/data/response.csv") 
        df=df[df["Leave"]!=0]   
        df.to_csv("/home/sudip/airflow/data/clean.csv", index=False) 
    except:
        print("error while preprocessing!!!!!!")    
    
with DAG(dag_id="API-DATA-FETCH",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:
    
        fetch = PythonOperator(
            task_id="Fetch_from_API",
            python_callable=fetch_from_api)
        preprocess = PythonOperator(task_id="preprocess",
            python_callable=preprocess
            )
fetch >> preprocess