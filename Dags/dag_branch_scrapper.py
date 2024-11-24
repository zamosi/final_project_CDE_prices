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
    'email': ['alexeyserdtse@gmail.com'], 
    'email_on_failure': True, 
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1), 
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

    # Task to validate the schema in the database by executing a Python script over SSH
    validation = SSHOperator(
        task_id='Validate_Schema_In_DB',
        ssh_conn_id='ssh_default',
        command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Tests/validate_schema.py'
    )

    # Task to execute the branch scraper script over SSH to scrape data from branches
    run_scraper = SSHOperator(
        task_id='run_branch_scraper',
        ssh_conn_id='ssh_default',
        command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Scrappers/branch_scraper_all_files.py'
    )

    # Steps of the DAG
    validation >> run_scraper