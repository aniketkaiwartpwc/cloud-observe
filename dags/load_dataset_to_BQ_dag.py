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

def task_conditional_check(): 
    task_id: Literal['t3_check_dataset_product'] = 't3_check_dataset_product'
    if task_id=='t3_check_dataset_product':
        return 't3_check_dataset_product'
    elif task_id=='t3_check_dataset_customer':
        return 't3_check_dataset_customer'
    elif task_id=='t3_check_dataset_employee':
        return 't3_check_dataset_employee'
    elif task_id=='t3_check_dataset_sales':
        return 't3_check_dataset_sales'
    elif task_id=='t3_check_dataset_gender':
        return 't3_check_dataset_gender'

def transformation_conditional_check(): 
    task_id: Literal['t4_transform_table_product'] = 't4_transform_table_product'
    if task_id=='t4_transform_table_product':
        return 't4_transform_table_product'
    elif task_id=='t4_transform_table_customer':
        return 't4_transform_table_customer'
    elif task_id=='t4_transform_table_employee':
        return 't4_transform_table_employee'
    elif task_id=='t4_transform_table_sales':
        return 't4_transform_table_sales'
    elif task_id=='t4_transform_table_gender':
        return 't4_transform_table_gender'

def dataset_conditional_check(): 
    task_id: Literal['t5_check_dataset_product'] = 't5_check_dataset_product'
    if task_id=='t5_check_dataset_product':
        return 't5_check_dataset_product'
    elif task_id=='t5_check_dataset_customer':
        return 't5_check_dataset_customer'
    elif task_id=='t5_check_dataset_employee':
        return 't5_check_dataset_employee'
    elif task_id=='t5_check_dataset_sales':
        return 't5_check_dataset_sales'
    elif task_id=='t5_check_dataset_gender':
        return 't5_check_dataset_gender'
    
with models.DAG(
        "load_dataset_to_BQ_dag",
        default_args=default_args,
        start_date=datetime.datetime(2024, 1, 8),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval='0 6 * * 4-5',
) as dag:
    
    #data check before transformation
    t3_check_dataset_product = BigQueryCheckOperator(
        task_id = 't3_check_dataset_product',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.product_table`'
        )
    

    t3_check_dataset_customer = BigQueryCheckOperator(
        task_id = 't3_check_dataset_customer',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.customer_table`'
        )
    
    t3_check_dataset_employee = BigQueryCheckOperator(
        task_id = 't3_check_dataset_employee',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.employee_table`'
        )
    

    t3_check_dataset_sales = BigQueryCheckOperator(
        task_id = 't3_check_dataset_sales',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.sales_table`'
        )
    
    t3_check_dataset_gender = BigQueryCheckOperator(
        task_id = 't3_check_dataset_gender',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.gender_table`'
        )
    
    # BQ_Transformation
    BQ_Table_Transformation = BranchPythonOperator(
        task_id = 'BQ_Table_Transformation',
        dag = dag,
        python_callable=transformation_conditional_check,
        do_xcom_push=False,
        trigger_rule='none_failed_min_one_success'
        )
    
    t4_transform_table_product = BigQueryOperator(
        task_id = 't4_transform_table_product',
        use_legacy_sql = False,
        location = LOCATION,
        sql = '/SQL/product_transform_table.sql'
        )

    t4_transform_table_customer = BigQueryOperator(
        task_id = 't4_transform_table_customer',
        use_legacy_sql = False,
        location = LOCATION,
        sql = '/SQL/customer_transform_table.sql'
        )

    t4_transform_table_employee = BigQueryOperator(
        task_id = 't4_transform_table_employee',
        use_legacy_sql = False,
        location = LOCATION,
        sql = '/SQL/employee_transform_table.sql'
        )

    t4_transform_table_sales = BigQueryOperator(
        task_id = 't4_transform_table_sales',
        use_legacy_sql = False,
        location = LOCATION,
        sql = '/SQL/sales_transform_table.sql'
        )    

    t4_transform_table_gender = BigQueryOperator(
        task_id = 't4_transform_table_gender',
        use_legacy_sql = False,
        location = LOCATION,
        sql = '/SQL/gender_transform_table.sql'
        )
    
    #data check after transformation

    # BQ_Transformation
    Transformation_Check = BranchPythonOperator(
        task_id = 'Transformation_Check',
        dag = dag,
        python_callable=dataset_conditional_check,
        do_xcom_push=False,
        trigger_rule='none_failed_min_one_success'
        )

    t5_check_dataset_product = BigQueryCheckOperator(
        task_id = 't5_check_dataset_product',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.product_transform_table`'
        )
    
    t5_check_dataset_customer = BigQueryCheckOperator(
        task_id = 't5_check_dataset_customer',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.customer_transform_table`'
        )
    
    t5_check_dataset_employee = BigQueryCheckOperator(
        task_id = 't5_check_dataset_employee',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.employee_transform_table`'
        )
    
    t5_check_dataset_sales = BigQueryCheckOperator(
        task_id = 't5_check_dataset_sales',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.sales_transform_table`'
        )


    t5_check_dataset_gender = BigQueryCheckOperator(
        task_id = 't5_check_dataset_gender',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.dataobs.gender_transform_table`'
        )
    

    
    t6_trigger_archived_dag = TriggerDagRunOperator(
        task_id='t6_trigger_archived_dag',
        trigger_dag_id='archived_file_dag',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        trigger_rule='none_failed_min_one_success'
    )
    

    start = BranchPythonOperator(
        task_id='start',
        python_callable=task_conditional_check,
        do_xcom_push=False
        )
    end = DummyOperator(task_id='end')


    start >> [t3_check_dataset_product,t3_check_dataset_customer,t3_check_dataset_employee,t3_check_dataset_sales,t3_check_dataset_gender]
    [t3_check_dataset_product,t3_check_dataset_customer,t3_check_dataset_employee,t3_check_dataset_sales,t3_check_dataset_gender] >> BQ_Table_Transformation 
    BQ_Table_Transformation >> [t4_transform_table_product,t4_transform_table_customer,t4_transform_table_employee,t4_transform_table_sales,t4_transform_table_gender]
    [t4_transform_table_product,t4_transform_table_customer,t4_transform_table_employee,t4_transform_table_sales,t4_transform_table_gender] >> Transformation_Check
    Transformation_Check >> [t5_check_dataset_product,t5_check_dataset_customer,t5_check_dataset_employee,t5_check_dataset_sales,t5_check_dataset_gender]
    [t5_check_dataset_product,t5_check_dataset_customer,t5_check_dataset_employee,t5_check_dataset_sales,t5_check_dataset_gender] >> t6_trigger_archived_dag >> end 