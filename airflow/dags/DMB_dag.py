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
        bash_command=f"python3 {execution_path}/data_ingestion/data_ingestion.py --api_endpoint https://opendata-ajuntament.barcelona.cat/data/dataset/ee7ea708-d341-4f1c-bb2e-14199c197435/resource/5c0dd0e5-158c-45e2-bc78-22c32c9ed6b0/download --temporal_landing_zone_path {execution_path}/temporal_landing_zone --filename estacionaments_area_dum"
    )

    data_ingestion_2 = BashOperator(
        task_id='data_ingestion_2',
        bash_command=f"python3 {execution_path}/data_ingestion/data_ingestion.py --api_endpoint https://opendata-ajuntament.barcelona.cat/data/dataset/61736e58-fa50-48f1-812e-6bab525fc1d2/resource/3137a205-e46b-4570-a39d-fa44bd556669/download --temporal_landing_zone_path {execution_path}/temporal_landing_zone --filename oferta_publica_aj_bcn"
    )

    data_ingestion_3 = BashOperator(
        task_id='data_ingestion_3',
        bash_command=f"python3 {execution_path}/data_ingestion/data_ingestion.py --api_endpoint https://opendata-ajuntament.barcelona.cat/data/dataset/2767159c-1c98-46b8-a686-2b25b40cb053/resource/59b9c807-f6c1-4c10-ac51-1ace65485079/download --temporal_landing_zone_path {execution_path}/temporal_landing_zone --filename agenda_cultural_barcelona"
    )    

    persistent_landing_zone = BashOperator(
        task_id='persistent_landing_zone',
        bash_command=f"python3 {execution_path}/persistent_landing_zone/load_data_to_gcs.py --bucket_name persistent-landing-zone --folder_path {execution_path}/temporal_landing_zone --destination_folder persistent"
    )

    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> [data_ingestion_1, data_ingestion_2, data_ingestion_3] >> persistent_landing_zone