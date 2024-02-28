import datetime

import airflow
from airflow.operators import bash_operator
from airflow import models
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator,  BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import Literal
from airflow.operators.python import BranchPythonOperator
import time



import google.auth
from google.cloud import storage
import pandas as pd
from google.oauth2 import service_account
import os


YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
PROJECT_ID="pg-us-n-app-119329"
LOCATION = "asia-south1"

# credentials_path = "gs://dataobs/raw-data/pg-us-n-app-119329-70119b0a0801.json"

# credentials = service_account.Credentials.from_service_account_file(

#     credentials_path,

#     scopes=["https://www.googleapis.com/auth/cloud-platform"]

# )

# credentials, project = google.auth.default(
#     scopes=['https://www.googleapis.com/auth/cloud-platform'])
credentials, project = google.auth.default()
client = storage.Client(credentials=credentials)

default_args = {
    'project': 'pg-us-n-app-119329',
    'owner': 'aniket',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}


with models.DAG(
        "insert_employee_tbl_count_dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 8),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval=None,
) as dag:
    
    # def delay():
    #  time.sleep(600)

    time_now = datetime.datetime.now()


    employee_tbl_name = "raw-data/employee_data.csv"
    bucket_name = "dataobs"
    def table_count(bucket_name,file_name):
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        if blob.exists():
            file_path = 'gs://'+bucket_name + '/' + file_name
            df = pd.read_csv(file_path)
            tbl_count = len(df)
            return tbl_count, 'FileExists'
        else:
            return 0, 'FileNotFound'

    employee_tbl_count, employee_tbl_comments = table_count(bucket_name,employee_tbl_name)

    INSERT_ROWS_QUERY_EMPLOYEE = (
        f"INSERT `pg-us-n-app-119329.airflow_log_capture.daily_source_table_count` VALUES ('employee', {employee_tbl_count}, '{time_now}','{employee_tbl_comments}');"

    )

    employee_insert_query_job = BigQueryInsertJobOperator(
    task_id="employee_insert_query_job",
    configuration={
        "query": {
            "query": INSERT_ROWS_QUERY_EMPLOYEE,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    location=LOCATION,
    )
    
    jobs_started = DummyOperator(
    task_id = 'jobs_started',
    dag = dag
    )
    jobs_ended = DummyOperator(
    task_id = 'jobs_ended',
    dag = dag
    )
    jobs_started >> employee_insert_query_job >> jobs_ended