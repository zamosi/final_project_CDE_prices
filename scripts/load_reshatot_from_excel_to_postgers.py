# Standard Library Imports
import sys
import os
import io
import logging
from configparser import ConfigParser
from datetime import datetime

# Third-Party Libraries
import pandas as pd

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import connect_to_postgres_data
from Connections.connection import init_minio_client

# Set up Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

def insert_dataframe_to_postgres(engine, df: pd.DataFrame, table_name: str):
    """
    Uses the SQLAlchemy engine to insert data into a PostgreSQL database. The data
    is inserted into the specified table under the 'raw_data' schema. Existing
    data in the table is replaced.

    """
    try:
        df.to_sql(table_name, con=engine, schema='raw_data', if_exists='replace', index=False)
        logger.info(f"Data inserted successfully into {table_name}.")
    except Exception as e:
        logger.error(f"Error inserting data into PostgreSQL: {e}")


def check_file_exists(path:str) -> bool:
    """
    Validates if a file exists at the given path.
    
    Returns:
        bool: True if the file exists, False otherwise.
    """
    return os.path.isfile(path)


def append_to_parquet(new_data: pd.DataFrame, parquet_path: str):
    """
    Appends new data to an existing Parquet file. If the file does not exist, it creates a new one.
    """
    if os.path.exists(parquet_path):
        existing_data = pd.read_parquet(parquet_path)

        # Concatenate the new data with existing data
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)

        # Write back to Parquet
        combined_data.to_parquet(parquet_path, index=False, compression='snappy')
    else:
        # Write new data if the file doesn't exist
        new_data.to_parquet(parquet_path, index=False, compression='snappy')
        print(f"New Parquet file created at {parquet_path}.")


def upload_to_minio(minio_client, bucket_name, folder_name, file_path):
    """
    Uploads a file to a specified folder in a MinIO bucket.
    """
    try:
        # Construct the destination path in the bucket
        file_name = os.path.basename(file_path)
        object_name = os.path.join(folder_name, file_name)

        # Upload the file
        minio_client.fput_object(bucket_name, object_name, file_path)
        logger.info(f"File '{file_path}' uploaded to '{bucket_name}/{object_name}'.")
    except Exception as e:
        logger.error(f"Failed to upload file to MinIO: {e}")


def main():
    # Define path to Excel with stores
    excel_path = config["Core_Settings"]["MARKETS_DETAILS_EXCEL_PATH"]

    # Init Minio client
    minio_client = init_minio_client()
    bucket_name = 'prices'
    dest_folder_name = 'markets'

    # Define Parquet file path
    source_folder = config["Core_Settings"]["SOURCE_FOLDER_MARKETS"]
    parquet_file_path = os.path.join(source_folder, f"{dest_folder_name}.parquet")


    # Validate file existence
    if check_file_exists(excel_path):
        df = pd.read_excel(excel_path)
        df['run_time'] = datetime.now()
        df['password'] = df['password'].astype(str).str.strip()
        conn, engine = connect_to_postgres_data()
        insert_dataframe_to_postgres(engine, df, 'reshatot')


        # Append to Parquet
        # append_to_parquet(df, parquet_file_path)

        # # Upload Parquet file to MinIO
        # upload_to_minio(minio_client, bucket_name, dest_folder_name, parquet_file_path)
    else:
        logger.critical("File doesn't exist in the given path, please set the file first in the folder!")


if __name__ == '__main__':
    main()