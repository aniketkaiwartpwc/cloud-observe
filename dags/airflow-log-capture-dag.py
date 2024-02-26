import datetime

import airflow
from airflow.operators import bash_operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import models
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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
        "airflow-log-capture-dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 29),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval='0 8 * * 1-5',
) as dag:
    t1_dataflow_job_file_to_bq_log_daily_count = DataflowCreatePythonJobOperator(
        task_id="t1_dataflow_job_file_to_bq_log_daily_count",
        py_file="gs://asia-south1-cloud-dataobs-359986fe-bucket/dataflow-functions/log_table_generator.py",
        job_name="mobile-price-file-to-bq-raw",
        options = {
                'project': 'pg-us-n-app-119329'
                },
        dataflow_default_options = {
            "temp_location": "gs://dataobs/tmp/",
            "staging_location": "gs://dataobs/staging-data/",
            "region": "asia-south1"
            }

    )

    t2_dataflow_job_file_to_bq_log_record_count = DataflowCreatePythonJobOperator(
        task_id="t2_dataflow_job_file_to_bq_log_record_count",
        py_file="gs://asia-south1-cloud-dataobs-359986fe-bucket/dataflow-functions/record_count_table.py",
        job_name="mobile-price-file-to-bq-raw",
        options = {
                'project': 'pg-us-n-app-119329'
                },
        dataflow_default_options = {
            "temp_location": "gs://dataobs/tmp/",
            "staging_location": "gs://dataobs/staging-data/",
            "region": "asia-south1"
            }

    )

    t3_dataflow_job_file_to_bq_log_daily_count_dag_level = DataflowCreatePythonJobOperator(
        task_id="t3_dataflow_job_file_to_bq_log_daily_count_dag_level",
        py_file="gs://asia-south1-cloud-dataobs-359986fe-bucket/dataflow-functions/log_table_dag_level.py",
        job_name="mobile-price-file-to-bq-raw",
        options = {
                'project': 'pg-us-n-app-119329'
                },
        dataflow_default_options = {
            "temp_location": "gs://dataobs/tmp/",
            "staging_location": "gs://dataobs/staging-data/",
            "region": "asia-south1"
            }

    )

    

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

start >> t1_dataflow_job_file_to_bq_log_daily_count >> t2_dataflow_job_file_to_bq_log_record_count >> t3_dataflow_job_file_to_bq_log_daily_count_dag_level >> end
