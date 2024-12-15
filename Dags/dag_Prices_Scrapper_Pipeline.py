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
    'retry_delay': timedelta(minutes=5), 
}

# Define the DAG
with DAG(
    dag_id='Prices_Scrapper_Pipeline',
    default_args=default_args,
    description='A DAG to run branch_scraper.py daily at 10:00 PM',
    schedule_interval='0 20 * * *',
    start_date=datetime(2024, 12, 15),
    catchup=False,
    tags=['scrapper', 'daily']
) as dag:

    # Task group for validation tasks
    with TaskGroup("Validation_Tasks") as validation_tasks:

        # Task to validate the schema in the database.
        Validate_Schema = SSHOperator(
            task_id='Validate_Schema_In_DB',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Tests/validate_schema.py'
        )

        # Task to validate the minio buckets.
        Validate_Buckets = SSHOperator(
            task_id='Validate_MinIO_Buckets',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Tests/validate_minio.py'
        )

    # Task group for scrapper tasks
    with TaskGroup("Scrapper_Tasks") as scrapper_tasks:

        # Task loads markets list into Postgres
        Load_Markets = SSHOperator(
            task_id='Load_Markets_List',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/scripts/load_reshatot_from_excel_to_postgers.py'
        )   

        # Daily scrapper
        Run_Scraper = SSHOperator(
            task_id='Run_Rranch_Scraper',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Scrappers/branch_scraper_all_files.py'
        )  

        # Scrapper tasks sequence
        Load_Markets >> Run_Scraper

    # Task group for producers tasks
    with TaskGroup("Producers_Tasks") as producers_tasks:

        # Task to produce snifim data
        Snifim_Producer = SSHOperator(
            task_id='Snifim_Producer',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/snifim_procuder.py'
        )

        # Task to produce prices data
        Prices_Producer = SSHOperator(
            task_id='Prices_Producer',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/prices_procuder.py'
        )

    # Task group for consumers tasks
    with TaskGroup("Consumers_Tasks") as consumers_tasks:

        # Task to consume prices data
        Prices_Data_Consumer = SSHOperator(
            task_id='Prices_Data_Consumer',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/consumer_prices_data.py'
        )

        # Task to consume scd data
        Prices_Scd = SSHOperator(
            task_id='Prices_Scd',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/consumer_to_postgres.py'
        )

        # Consumers tasks sequence
        Prices_Data_Consumer >> Prices_Scd
        
    with TaskGroup("Delete_Old_And_Archiving") as delete_and_archiving:
        # Task to delete data older than 7 days
        Delete_Old_Data = SSHOperator(
            task_id='Delete_Old_Data',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/delete_old_data.py'
        )
        
        # Task to zip data older than 7 days prices bucket
        Archive_Old_Data = SSHOperator(
            task_id='Archive_Old_Data',
            ssh_conn_id='ssh_default',
            command='python3 /home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Pipeline/archive_old_data.py'
        )

    # Steps of the DAG
    [Validate_Schema, Validate_Buckets] >> scrapper_tasks >> [Snifim_Producer, Prices_Producer] >> consumers_tasks >> [Delete_Old_Data, Archive_Old_Data]