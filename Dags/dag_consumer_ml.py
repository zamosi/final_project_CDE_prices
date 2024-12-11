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
    dag_id = 'consumer_to_ml',
    default_args = default_args,
    description = 'A DAG to run branch_scraper.py daily at 1:00 AM',
    schedule_interval = None,  # Runs daily at 1:00 AM
    start_date = datetime(2024, 12, 10),
    catchup = False,
    tags=['kafka', 'consumer', 'postgres']
) as dag:


    check_topics = SSHOperator(
        task_id='check_topics',
        ssh_conn_id='ssh_default',
        command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Tests/check_topics.py'
    )
    
    ml_consumer = SSHOperator(
        task_id='ml_consumer',
        ssh_conn_id='ssh_default',
        command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/consumer_to_ml.py'
    )

    # Steps of the DAG
    check_topics >> ml_consumer