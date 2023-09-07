from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from pathlib import Path
import sys, os

sys.path.append('../../')  # Replace with the actual path to the data_ingestion directory

execution_path = os.environ['DMB_EXECUTION_PATH']

# Default parameters for the workflow
default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'Data_Management_Backbone', # Name of the DAG / workflow
        default_args=default_args,
        catchup=False,
        schedule='*/15 * * * *' 
) as dag:
    # This operator does nothing. 
    start_task = EmptyOperator(
        task_id='start_task', # The name of the sub-task in the workflow.
        dag=dag # When using the "with Dag(...)" syntax you could leave this out
    )

    # With the PythonOperator you can run a python function.
    data_ingestion_1 = BashOperator(
        task_id='data_ingestion_1',
        bash_command=f"python3 {execution_path}/data_ingestion/data_ingestion.py --api_endpoint https://opendata.l-h.cat/resource/qjcy-xqxx.csv --temporal_landing_zone_path {execution_path}/temporal_landing_zone --filename BiciBox"
    )

    data_ingestion_2 = BashOperator(
        task_id='data_ingestion_2',
        bash_command=f"python3 {execution_path}/data_ingestion/data_ingestion.py --api_endpoint https://opendata.l-h.cat/resource/qtv3-9x52.csv --temporal_landing_zone_path {execution_path}/temporal_landing_zone --filename agenda_publica_de_la_ciutat_hp"
    )

    data_ingestion_3 = BashOperator(
        task_id='data_ingestion_3',
        bash_command=f"python3 {execution_path}/data_ingestion/data_ingestion.py --api_endpoint https://opendata.l-h.cat/resource/csm2-emdb.csv --temporal_landing_zone_path {execution_path}/temporal_landing_zone --filename qualitat_aire_hp"
    )    

    persistent_landing_zone = BashOperator(
        task_id='persistent_landing_zone',
        bash_command=f"python3 {execution_path}/persistent_landing_zone/load_data_to_gcs.py --bucket_name persistent-landing-zone --folder_path {execution_path}/temporal_landing_zone --destination_folder persistent"
    )

    formatted_zone = BashOperator(
        task_id='formatted_zone',
        bash_command=f"python3 {execution_path}/formatted_zone/move_data_to_formatted_zone.py --bucket persistent-landing-zone"
    )

    trusted_zone = BashOperator(
        task_id='trusted_zone',
        bash_command=f"python3 {execution_path}/trusted_zone/trusted_zone.py"
    )

    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> [data_ingestion_1, data_ingestion_2, data_ingestion_3] >> persistent_landing_zone >> formatted_zone >> trusted_zone