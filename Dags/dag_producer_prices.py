# Standard Library Imports
from datetime import datetime, timedelta

# Third-Party Libraries
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email import EmailOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True, 
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5), 
}

# Define the DAG
with DAG(
    dag_id = 'producer_prices',
    default_args = default_args,
    description = 'A DAG to run branch_scraper.py daily at 1:00 AM',
    schedule_interval = None,  # Runs daily at 1:00 AM
    start_date = datetime(2024, 12, 10),
    catchup = False,
    tags=['kafka', 'producer', 'prices']
) as dag:

    # Task to validate the schema in the database by executing a Python script over SSH
    prices_producer = SSHOperator(
        task_id='prices_producer',
        ssh_conn_id='ssh_default',
        command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/prices_procuder.py'
    )

    # Steps of the DAG
    prices_producer