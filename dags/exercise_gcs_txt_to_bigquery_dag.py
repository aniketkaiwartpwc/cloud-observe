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

    # [START dag_dependencies]
    trigger_next_dag: TriggerDagRunOperator(
        task_id="keyword_search_gcs_to_bigquery_job",
        trigger_dag_id ="keywords_search_dag.py", #ID of the dag to be created
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=30,
    )
    # [END dag_dependencies]

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> txt_bq_dataflow_job >> trigger_next_dag >> end

txt_to_bigquery_dataflow_etl = exercise_gcs_txt_to_bigquery_dag(),
