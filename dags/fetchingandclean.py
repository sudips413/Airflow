import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import csv
import json
def fetchData():
    try:
        url = "https://server-mav2.onrender.com/data"
        response = requests.request("GET", url)
        data= response.text
        data = json.loads(data)
        flocation="/home/sudip/airflow/data/surveyData/surveyData.csv"
        if not os.path.exists(flocation):
            open(flocation,'w').close()
            with open(flocation, 'w', newline='') as f:
            # Create a csv writer object
                writer = csv.writer(f)                
                # Write the header row
                writer.writerow(data[0].keys())                
                # Write the data rows
                for row in data:
                    writer.writerow(row.values())
        else:
            with open(flocation, 'w', newline='') as f:
            # Create a csv writer object
                writer = csv.writer(f)                
                # Write the header row
                writer.writerow(data[0].keys())                
                # Write the response.text rows
                for row in data:                    
                    writer.writerow(row.values())
    except:
        print("error while fetching")


def cleanData():
    try:
        flocation="/home/sudip/airflow/data/surveyData/surveyDataClean.csv"
        if not os.path.exists(flocation):
            open(flocation,'w').close()
        df= pd.read_csv("/home/sudip/airflow/data/surveyData/surveyData.csv")
        #remove duplicates
        df = df.drop('__v', axis=1)
        df = df.rename(columns={'_id': 'userID'})
        df= df.drop_duplicates()
        df.to_csv(flocation,index=False)
    except:
        print("Couldnot clean the data!!!")
with DAG(dag_id="SURVEY-DATA",
        start_date=datetime(2023,3,25),
        schedule_interval="@hourly",
        catchup=False) as dag:    
        fetch = PythonOperator(
            task_id="Fetch_from_API",
            python_callable=fetchData
        )
        clean = PythonOperator(
            task_id="clean_the_data",
            python_callable=cleanData
        )
fetch >> clean