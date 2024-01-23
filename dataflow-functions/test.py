import datetime

import airflow
from airflow.operators import bash_operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import models
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

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
        "test_dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 8),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval='0 6 * * 4-5',
) as dag:
    
    #data check before transformation
    t3_check_dataset = BigQueryCheckOperator(
        task_id = 't3_check_dataset',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.flipkart_mobile_prices`'
        )
    # BQ_Transformation
    BQ_Table_Transformation = DummyOperator(
        task_id = 'BQ_Table_Transformation',
        dag = dag
        )
    
    t4_transform_table = BigQueryOperator(
        task_id = 't4_transform_table',
        use_legacy_sql = False,
        location = LOCATION,
        sql = '/SQL/flipkart_mobile_prices_transform.sql'
        )
    
    #data check after transformation
    t5_check_dataset = BigQueryCheckOperator(
        task_id = 't5_check_dataset',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.flipkart_mobile_prices_transform`'
        )
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')


    start >> t3_check_dataset
    t3_check_dataset >> BQ_Table_Transformation 
    BQ_Table_Transformation >> t4_transform_table
    t4_transform_table >> t5_check_dataset
    t5_check_dataset >> end 