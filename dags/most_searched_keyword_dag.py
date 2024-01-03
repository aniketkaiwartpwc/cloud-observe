import os
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import configuration
from airflow.models import Variable

default_args = {
    'owner': 'aniket',
    'email': 'aniket.kaiwart@pwc.com',
}

COMPOSER_BUCKET_NAME = Variable.get('COMPOSER_BUCKET_NAME')
BUCKET_NAME = Variable.get('BUCKET_NAME')
OUTPUT = Variable.get('MOST_SEARCHED_KEYWORDS_BQ_OUTPUT_TABLE')
PY_FILE = (
    f'gs://{COMPOSER_BUCKET_NAME}/dataflow-functions/bigquery_to_bigquery.py' if Variable.get('ENVIRONMENT') == 'Production'
    else f"{os.path.dirname(configuration.conf.get('core', 'dags_folder'))}/dataflow-functions/bigquery_to_bigquery.py")

PROJECT_ID = Variable.get('PROJECT_ID')
GCS_TEMP_LOCATION = Variable.get('GCS_TEMP_LOCATION')
GCS_STG_LOCATION = Variable.get('GCS_STG_LOCATION')
KEYWORDS_BQ_TABLE = Variable.get('ALL_KEYWORDS_BQ_OUTPUT_TABLE')
INPUT_TABLE = f'{PROJECT_ID}:{KEYWORDS_BQ_TABLE}'


@dag(
    schedule=None,
    default_args=default_args,
    catchup=False,
    start_date=days_ago(1),
    tags=['dataflow-job']
)
def most_searched_keyword_dag():
    pipeline_options = {
        'tempLocation': GCS_TEMP_LOCATION,
        'inputTable': INPUT_TABLE,
        'output': OUTPUT,
        'stagingLocation': GCS_STG_LOCATION,
        'project': PROJECT_ID,
    }

    dataflow_task = BeamRunPythonPipelineOperator(
        task_id='get_most_searched_keyword',
        runner='DataflowRunner',
        gcp_conn_id='google_cloud_default',
        py_file=PY_FILE,
        py_requirements=['apache-beam[gcp]==2.29.0'],
        py_system_site_packages=True,
        py_interpreter='python3',
        pipeline_options=pipeline_options,
        dataflow_config=DataflowConfiguration(
            project_id=PROJECT_ID,
            location="asia-south1",
            wait_until_finished=True
        )
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_events_dag',
        trigger_dag_id='reverse_transactional_events_dag',
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=30,
    )


    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> dataflow_task >> trigger_next_dag >> end


most_searched_keyword_etl = most_searched_keyword_dag()
