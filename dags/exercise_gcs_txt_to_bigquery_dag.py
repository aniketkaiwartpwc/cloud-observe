import json
import os
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
#from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator -- This class is deprecated, use DataflowCreatePythonJobOperator instead
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

default_args = {
    'owner': 'aniket',
    'email': ['aniket.kaiwart@pwc.com'],
}


@dag(
    schedule=None,
    default_args=default_args,
    catchup=False,
    start_date=days_ago(1),
    tags=['dataflow-job']
)



def exercise_gcs_txt_to_bigquery_dag():
    txt_bq_dataflow_job = DataflowCreatePythonJobOperator(
        task_id='etl_txt_to_bq_dataflow_job',
        py_file='gs://asia-south1-cloud-dataobs-359986fe-bucket/dataflow-functions/process_citizen_txt.py',
        job_name='{{task.task_id}}',
        py_interpreter='python3',
        location='asia-south1'
    )


    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> txt_bq_dataflow_job >> end

txt_to_bigquery_dataflow_etl = exercise_gcs_txt_to_bigquery_dag(),
