import json
import os
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
        py_file='gs://asia-south1-cloud-dataobs-cf40bf82-bucket/dataflow-functions/process_citizen_txt.py',
        job_name='{{task.task_id}}',
        py_interpreter='python3',
        location='asia-south1'
    )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")



    # [START dag_dependencies]
    keyword_search_gcs_to_bigquery_job: TriggerDagRunOperator(
        task_id="keyword_search_gcs_to_bigquery_job",
        trigger_dag_id ="keywords_search_dag.py", #ID of the dag to be created
        execution_date = '{{ ds }}', #ds is built in airflow variable to set the execution date as YYYY-MM-DD
        reset_dag = True,   #For multiple run
        wait_for_completion = True,   #Ensure the task waits for completion of the dag being triggered
        poke_interval = 3 # check whether triggered dag is completed every 3 seconds
    )
    # [END dag_dependencies]

    start >> txt_bq_dataflow_job >> keyword_search_gcs_to_bigquery_job >> end


txt_to_bigquery_dataflow_etl = exercise_gcs_txt_to_bigquery_dag()
