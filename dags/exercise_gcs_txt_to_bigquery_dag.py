import json
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
#from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator -- This class is deprecated, use DataflowCreatePythonJobOperator instead
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow import configuration

default_args = {
    'owner': 'aniket',
    'email': ['aniket.kaiwart@pwc.com'],
}


@dag(default_args=default_args, schedule_interval='0 6 * * *', start_date=days_ago(1), tags=['dataflow-job'])
def exercise_gcs_txt_to_bigquery_dag():
    txt_bq_dataflow_job = DataflowCreatePythonJobOperator(
        task_id='etl_txt_to_bq_dataflow_job',
        py_file='gs://asia-south1-cloud-dataobs-1bb414f6-bucket/dataflow-functions/process_citizen_txt.py',
        job_name='{{task.task_id}}',
        py_interpreter='python3',
        location='asia-south1'
    )


txt_to_bigquery_dataflow_etl = exercise_gcs_txt_to_bigquery_dag()
