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

def task_conditional_check(): 
    job_name: Literal['product-file-to-bq-raw'] = 'product-file-to-bq-raw'
    if job_name=='product-file-to-bq-raw':
        return 't1_dataflow_job_file_to_bq_product'
    elif job_name=='customer-file-to-bq-raw':
        return 't1_dataflow_job_file_to_bq_customer'
    elif job_name=='employee-file-to-bq-raw':
        return 't1_dataflow_job_file_to_bq_employee'
    elif job_name=='sales-file-to-bq-raw':
        return 't1_dataflow_job_file_to_bq_sales'
    elif job_name=='gender-file-to-bq-raw':
        return 't1_dataflow_job_file_to_bq_gender'

def insert_conditional_check(): 
    task_id: Literal['t2_insert_product_table'] = 't2_insert_product_table'
    if task_id=='t2_insert_product_table':
        return 't2_insert_product_table'
    elif task_id=='t2_insert_customer_table':
        return 't2_insert_customer_table'
    elif task_id=='t2_insert_employee_table':
        return 't2_insert_employee_table'
    elif task_id=='t2_insert_gender_table':
        return 't2_insert_gender_table'
    elif task_id=='t2_insert_sales_table':
        return 't2_insert_sales_table'

with models.DAG(
        "dataset-ingest-to-GCS-dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 8),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval='0 6 * * 4-5',
) as dag:
    
    t0_task_conditional_check = BranchPythonOperator(
        task_id="t0_task_conditional_check",
        python_callable=task_conditional_check,
        do_xcom_push=False,
    )
    
    
    #dataset_ingest_to_GCS_dag Product
    t1_dataflow_job_file_to_bq_product = DataflowCreatePythonJobOperator(
        task_id="t1_dataflow_job_file_to_bq_product",
        py_file="gs://asia-south1-cloud-dataobs-c1d6a270-bucket/dataflow-functions/prod_tbl_function.py",
        job_name="product-file-to-bq-raw",
        options = {
                'project': 'pg-us-n-app-119329'
                },
        dataflow_default_options = {
            "inputfile": "gs://dataobs/raw-data/product_table.csv",
            "temp_location": "gs://dataobs/tmp/",
            "staging_location": "gs://dataobs/staging-data/",
            "region": "asia-south1"
            }

    )

    #dataset_ingest_to_GCS_dag Customer
    t1_dataflow_job_file_to_bq_customer = DataflowCreatePythonJobOperator(
        task_id="t1_dataflow_job_file_to_bq_customer",
        py_file="gs://asia-south1-cloud-dataobs-c1d6a270-bucket/dataflow-functions/customer_tbl_function.py",
        job_name="customer-file-to-bq-raw",
        options = {
                'project': 'pg-us-n-app-119329'
                },
        dataflow_default_options = {
            "inputfile": "gs://dataobs/raw-data/customer_data.csv",
            "temp_location": "gs://dataobs/tmp/",
            "staging_location": "gs://dataobs/staging-data/",
            "region": "asia-south1"
            }

    )

    #dataset_ingest_to_GCS_dag Employee
    t1_dataflow_job_file_to_bq_employee = DataflowCreatePythonJobOperator(
        task_id="t1_dataflow_job_file_to_bq_employee",
        py_file="gs://asia-south1-cloud-dataobs-c1d6a270-bucket/dataflow-functions/emp_tbl_function.py",
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

    #dataset_ingest_to_GCS_dag Sales
    t1_dataflow_job_file_to_bq_sales = DataflowCreatePythonJobOperator(
        task_id="t1_dataflow_job_file_to_bq_sales",
        py_file="gs://asia-south1-cloud-dataobs-c1d6a270-bucket/dataflow-functions/sales_tbl_function.py",
        job_name="sales-file-to-bq-raw",
        options = {
                'project': 'pg-us-n-app-119329'
                },
        dataflow_default_options = {
            "inputfile": "gs://dataobs/raw-data/sales_data.csv",
            "temp_location": "gs://dataobs/tmp/",
            "staging_location": "gs://dataobs/staging-data/",
            "region": "asia-south1"
            }

    )

    #dataset_ingest_to_GCS_dag Gender
    t1_dataflow_job_file_to_bq_gender = DataflowCreatePythonJobOperator(
        task_id="t1_dataflow_job_file_to_bq_gender",
        py_file="gs://asia-south1-cloud-dataobs-c1d6a270-bucket/dataflow-functions/gender_tbl_function.py",
        job_name="gender-file-to-bq-raw",
        options = {
                'project': 'pg-us-n-app-119329'
                },
        dataflow_default_options = {
            "inputfile": "gs://dataobs/raw-data/gender_data.csv",
            "temp_location": "gs://dataobs/tmp/",
            "staging_location": "gs://dataobs/staging-data/",
            "region": "asia-south1"
            }

    )

    insert_data_to_BQ = BranchPythonOperator(
        task_id = 'insert_data_to_BQ',
        dag = dag,
        python_callable=insert_conditional_check,
        do_xcom_push=False,
        trigger_rule='none_failed_min_one_success'
        ) 

        #load_dataset_to_BQ_dag

    t2_insert_product_table = BigQueryOperator(
        task_id='t2_insert_product_table',
        sql='/SQL/product_table.sql',
        use_legacy_sql=False,
    )

    t2_insert_employee_table = BigQueryOperator(
        task_id='t2_insert_employee_table',
        sql='/SQL/employee_table.sql',
        use_legacy_sql=False,
    )

    t2_insert_sales_table = BigQueryOperator(
        task_id='t2_insert_sales_table',
        sql='/SQL/sales_table.sql',
        use_legacy_sql=False,
    )

    t2_insert_customer_table = BigQueryOperator(
        task_id='t2_insert_customer_table',
        sql='/SQL/customer_table.sql',
        use_legacy_sql=False,
    )

    t2_insert_gender_table = BigQueryOperator(
        task_id='t2_insert_gender_table',
        sql='/SQL/gender_table.sql',
        use_legacy_sql=False,
    )

    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
    
    
    t3_trigger_next_dag = TriggerDagRunOperator(
        task_id='t3_trigger_next_dag',
        trigger_dag_id='load_dataset_to_BQ_dag',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        trigger_rule='none_failed_min_one_success'
    )
    
    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        )
    
start_pipeline >> t0_task_conditional_check
t0_task_conditional_check >> [t1_dataflow_job_file_to_bq_product, t1_dataflow_job_file_to_bq_customer,t1_dataflow_job_file_to_bq_employee,t1_dataflow_job_file_to_bq_sales,t1_dataflow_job_file_to_bq_gender]
t1_dataflow_job_file_to_bq_product >> insert_data_to_BQ
t1_dataflow_job_file_to_bq_customer >> insert_data_to_BQ
t1_dataflow_job_file_to_bq_employee >> insert_data_to_BQ
t1_dataflow_job_file_to_bq_sales >> insert_data_to_BQ
t1_dataflow_job_file_to_bq_gender >> insert_data_to_BQ
insert_data_to_BQ >> [t2_insert_product_table, t2_insert_customer_table,t2_insert_employee_table,t2_insert_sales_table,t2_insert_gender_table] 
[t2_insert_product_table, t2_insert_customer_table,t2_insert_employee_table,t2_insert_sales_table,t2_insert_gender_table]  >> t3_trigger_next_dag  
t3_trigger_next_dag >> finish_pipeline