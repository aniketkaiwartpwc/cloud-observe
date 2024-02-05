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
        "employee_load_dataset_to_BQ_dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 8),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval='0 6 * * 4-5',
) as dag:
    
    #data check before transformation
    t3_check_dataset_employee = BigQueryCheckOperator(
        task_id = 't3_check_dataset_employee',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.employee_table`'
        )
    
    t4_transform_table_employee = BigQueryOperator(
        task_id = 't4_transform_table_employee',
        use_legacy_sql = False,
        location = LOCATION,
        sql = '/SQL/employee_transform_table.sql'
        )
    
    t5_check_dataset_employee = BigQueryCheckOperator(
        task_id = 't5_check_dataset_employee',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.employee_transform_table`'
        )
    
    t6_trigger_archived_dag_employee = TriggerDagRunOperator(
        task_id='t6_trigger_archived_dag_employee',
        trigger_dag_id='employee_archived_file_dag',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        trigger_rule='none_failed_min_one_success'
    )

    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
    
    success = DummyOperator(
        task_id = 'success',
        dag = dag
        )
    
start_pipeline >> t3_check_dataset_employee
t3_check_dataset_employee >> t4_transform_table_employee
t4_transform_table_employee >> t5_check_dataset_employee
t5_check_dataset_employee >> t6_trigger_archived_dag_employee
t6_trigger_archived_dag_employee >> success  
    
