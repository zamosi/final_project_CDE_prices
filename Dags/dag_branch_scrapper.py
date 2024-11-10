from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alexeyserdtse@gmail.com'],  # Set your email for notifications
    'email_on_failure': True,  # Send email if the task fails
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),  # Retry delay of 1 minute
}

# Define the DAG
with DAG(
    dag_id='branch_info_scraper',
    default_args=default_args,
    description='A DAG to run branch_scraper.py daily at 1:00 AM',
    schedule_interval='0 1 * * *',  # Runs daily at 1:00 AM
    start_date=datetime(2024, 11, 9),
    catchup=False,
    tags=['scrapper', 'daily']
) as dag:

    # Task to run the branch scraper script
    run_scraper = SSHOperator(
        task_id='run_branch_scraper',
        ssh_conn_id='ssh_default',
        command='python3 /home/developer/projects/CDE_Final_Project/Scrappers/branch_scraper.py'
    )

    run_scraper