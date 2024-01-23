import datetime
from itertools import product

import airflow
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import models
from airflow.operators.dummy import DummyOperator
from typing import Literal
from airflow.operators.python import BranchPythonOperator

file_name_product="product_table.csv"
file_name_customer="customer_data.csv"
file_name_employee="employee_data.csv"
file_name_sales="sales_data.csv"
file_name_gender="gender_data.csv"


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

def task_conditional_check(): 
    task_id: Literal['t7_archive_file_product'] = 't7_archive_file_product'
    if task_id=='t7_archive_file_product':
        return 't7_archive_file_product'
    elif task_id=='t7_archive_file_customer':
        return 't7_archive_file_customer'
    elif task_id=='t7_archive_file_employee':
        return 't7_archive_file_employee'
    elif task_id=='t7_archive_file_sales':
        return 't7_archive_file_sales'
    elif task_id=='t7_archive_file_gender':
        return 't7_archive_file_gender'

with models.DAG(
        "archived_file_dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 8),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval='0 6 * * 4-5',
) as dag:
    
    t7_archive_file_product = GCSToGCSOperator(
        task_id="t7_archive_file_product",
        source_bucket="dataobs",
        source_object="product_table.csv",
        destination_bucket="pg-us-n-app-119329",
        destination_object="archive/"+str(datetime.date.today())+"-"+file_name_product,
        move_object=True,
    )

    t7_archive_file_customer = GCSToGCSOperator(
        task_id="t7_archive_file_customer",
        source_bucket="dataobs",
        source_object="customer_data.csv",
        destination_bucket="pg-us-n-app-119329",
        destination_object="archive/"+str(datetime.date.today())+"-"+file_name_customer,
        move_object=True,
    )  

    t7_archive_file_employee = GCSToGCSOperator(
        task_id="t7_archive_file_employee",
        source_bucket="dataobs",
        source_object="product_table.csv",
        destination_bucket="pg-us-n-app-119329",
        destination_object="archive/"+str(datetime.date.today())+"-"+file_name_employee,
        move_object=True,
    )

    t7_archive_file_sales = GCSToGCSOperator(
        task_id="t7_archive_file_sales",
        source_bucket="dataobs",
        source_object="customer_data.csv",
        destination_bucket="pg-us-n-app-119329",
        destination_object="archive/"+str(datetime.date.today())+"-"+file_name_sales,
        move_object=True,
    )  

    t7_archive_file_gender = GCSToGCSOperator(
        task_id="t7_archive_file_gender",
        source_bucket="dataobs",
        source_object="customer_data.csv",
        destination_bucket="pg-us-n-app-119329",
        destination_object="archive/"+str(datetime.date.today())+"-"+file_name_gender,
        move_object=True,
    )  


    start = BranchPythonOperator(
        task_id='start',
        python_callable=task_conditional_check,
        do_xcom_push=False,
    )
    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success'
        )

    start >> [t7_archive_file_product,t7_archive_file_customer,t7_archive_file_employee,t7_archive_file_sales,t7_archive_file_gender]
    [t7_archive_file_product,t7_archive_file_customer,t7_archive_file_employee,t7_archive_file_sales,t7_archive_file_gender] >> end
