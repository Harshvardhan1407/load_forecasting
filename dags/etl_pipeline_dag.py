from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
# import sys
import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
print("1,Current working directory: ", os.getcwd())
from component import ETL
print("2,Current working directory: ", os.getcwd())

# Initialize your ETL pipeline class
etl_pipeline_obj = ETL.ETL_PIPELINE()

default_args = {
    'owner': 'harsh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def start_etl_pipeline():
    # MongoDB connection
    # mongo_collection = etl_pipeline_obj.get_mongodb_connection(purpose="ingestion")
    # etl_pipeline_obj.start_pipeline(mongo_collection, process_all=True, max_workers=10)
    etl_pipeline_obj.test_pipeline()


with DAG(
    'etl_pipeline_dag',
    default_args=default_args,
    description='ETL Pipeline Orchestrated with Airflow',
    # schedule_interval=timedelta(hours=1),  # Adjust scheduling as needed
    start_date=datetime.now(),
    catchup=False,
) as dag:

    # Define ETL task
    etl_task = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=start_etl_pipeline,
        op_args="None",
        op_kwargs="None",
        templates_dict="None",
        templates_exts="None",
        show_return_value_in_logs="True",
    )
    etl_task


