# Standard Library Imports
from datetime import datetime, timedelta

# Third-Party Libraries
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True, 
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1), 
}

# Define the DAG
with DAG(
    dag_id='Prices_Scrapper_Pipeline',
    default_args=default_args,
    description='A DAG to run branch_scraper.py daily at 10:00 PM',
    schedule_interval='0 20 * * *',
    start_date=datetime(2024, 12, 14),
    catchup=False,
    tags=['scrapper', 'daily']
) as dag:

    # Task group for validation tasks
    with TaskGroup("Validation_Tasks") as validation_tasks:

        # Task to validate the schema in the database.
        validate_schema = SSHOperator(
            task_id='Validate_Schema_In_DB',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Tests/validate_schema.py'
        )

        # Task to validate the minio buckets.
        validate_buckets = SSHOperator(
            task_id='Validate_MinIO_Buckets',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Tests/validate_minio.py'
        )

    # Task group for scrapper tasks
    with TaskGroup("Scrapper_Tasks") as scrapper_tasks:

        # Task loads markets list into Postgres
        load_markets = SSHOperator(
            task_id='Load_Markets_List',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/scripts/load_reshatot_from_excel_to_postgers.py'
        )   

        # Daily scrapper
        run_scraper = SSHOperator(
            task_id='run_branch_scraper',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Scrappers/branch_scraper_all_files.py'
        )  

        # Scrapper tasks sequence
        load_markets >> run_scraper

    # Task group for producers tasks
    with TaskGroup("Producers_Tasks") as producers_tasks:

        # Task to produce snifim data
        snifim_producer = SSHOperator(
            task_id='snifim_producer',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/snifim_procuder.py'
        )

        # Task to produce prices data
        prices_producer = SSHOperator(
            task_id='prices_producer',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/prices_procuder.py'
        )

    # Task group for consumers tasks
    with TaskGroup("Consumers_Tasks") as consumers_tasks:

        # Task to consume prices data
        prices_data_consumer = SSHOperator(
            task_id='prices_data_consumer',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/consumer_prices_data.py'
        )
        
    with TaskGroup("Delete_Old_And_Archiving") as delete_and_archiving:
        # Task to delete data older than 7 days
        delete_old_data = SSHOperator(
            task_id='delete_old_data',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/delete_old_data.py'
        )

    # Steps of the DAG
    [validate_schema, validate_buckets] >> scrapper_tasks >> [snifim_producer, prices_producer] >> consumers_tasks >> [delete_old_data]
