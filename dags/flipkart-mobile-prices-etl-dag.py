import datetime

import airflow
from airflow.operators import bash_operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import models
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

file_name="flipkart_mobiles.csv"

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
        "flipkart-mobile-prices-etl-dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 8),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval='0 6 * * 1-3',
) as dag:
    t1_dataflow_job_file_to_bq = DataflowCreatePythonJobOperator(
        task_id="t1_dataflow_job_file_to_bq",
        py_file="gs://asia-south1-cloud-dataobs-359986fe-bucket/dataflow-functions/main_data.py",
        job_name="mobile-price-file-to-bq-raw",
        options = {
                'project': 'pg-us-n-app-119329'
                },
        dataflow_default_options = {
            "inputfile": "gs://dataobs/raw-data/flipkart_mobiles.csv",
            "temp_location": "gs://dataobs/tmp/",
            "staging_location": "gs://dataobs/staging-data/",
            "region": "asia-south1"
            }

    )

    t2_insert_mobile_price_into_trst_table = BigQueryOperator(
        task_id='t2_insert_mobile_price_into_trst_table',
        sql='/SQL/insert_mobile_price_trst.sql',
        use_legacy_sql=False,
    )

    t3_archive_file = GCSToGCSOperator(
        task_id="t3_archive_file",
        source_bucket="dataobs",
        source_object="flipkart_mobiles.csv",
        destination_bucket="pg-us-n-app-119329",
        destination_object="archive/"+str(datetime.date.today())+"-"+file_name,
        move_object=True,
    )

    t4_trigger_gcs_bq_dag = TriggerDagRunOperator(
        task_id='t4_trigger_gcs_bq_dag',
        trigger_dag_id='test_dag',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    start = DummyOperator(task_id='start')

start >> t1_dataflow_job_file_to_bq >> t2_insert_mobile_price_into_trst_table >> t3_archive_file >> t4_trigger_gcs_bq_dag
