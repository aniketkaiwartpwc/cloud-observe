import datetime
from itertools import product

import airflow
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import models
from airflow.operators.dummy import DummyOperator
from typing import Literal
from airflow.operators.python import BranchPythonOperator

file_name_employee="employee_data.csv"

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

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
        "employee_archived_file_dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 8),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval='0 6 * * 4-5',
) as dag:
    
    t7_archive_file_employee = GCSToGCSOperator(
        task_id="t7_archive_file_employee",
        source_bucket="dataobs",
        source_object="employee_data.csv",
        destination_bucket="pg-us-n-app-119329",
        destination_object="archive/"+str(datetime.date.today())+"-"+file_name_employee,
        move_object=True,
    )


    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
    
    success = DummyOperator(
        task_id = 'success',
        dag = dag
        )
    
start_pipeline >> t7_archive_file_employee 

t7_archive_file_employee >> success