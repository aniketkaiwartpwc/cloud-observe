import datetime

import airflow
from airflow.operators import bash_operator
from airflow import models
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import Literal
from airflow.operators.python import BranchPythonOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
PROJECT_ID="pg-us-n-app-119329"
LOCATION = "asia-south1"

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
        "employee_dataset-ingest-to-GCS-dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 8),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval='0 6 * * 4-5',
) as dag:
    
    
 
    #dataset_ingest_to_GCS_dag Product
    t1_dataflow_job_file_to_bq_employee = DataflowCreatePythonJobOperator(
        task_id="t1_dataflow_job_file_to_bq_employee",
        py_file="gs://asia-south1-cloud-dataobs-359986fe-bucket/dataflow-functions/emp_tbl_function.py",
        job_name="employee-file-to-bq-raw",
        options = {
                'project': 'pg-us-n-app-119329'
                },
        dataflow_default_options = {
            "inputfile": "gs://dataobs/raw-data/employee_data.csv",
            "temp_location": "gs://dataobs/tmp/",
            "staging_location": "gs://dataobs/staging-data/",
            "region": "asia-south1"
            }

    )

    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )


    t2_trigger_load_dataset_to_BQ_dag_employee = TriggerDagRunOperator(
      task_id='t2_trigger_load_dataset_to_BQ_dag_employee',
        trigger_dag_id='employee_load_dataset_to_BQ_dag',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        trigger_rule='none_failed_min_one_success'
    )
    
    success = DummyOperator(
        task_id = 'success',
        dag = dag
        )
    
    start_pipeline >> t1_dataflow_job_file_to_bq_employee

    t1_dataflow_job_file_to_bq_employee >> t2_trigger_load_dataset_to_BQ_dag_employee

    t2_trigger_load_dataset_to_BQ_dag_employee >> success
