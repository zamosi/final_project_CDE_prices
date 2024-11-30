# Standard Library Imports
import sys
import os
import logging
import re
from configparser import ConfigParser

# Third-Party Libraries
from minio import Minio
from minio.error import S3Error

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import init_minio_client


# Load configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")


# Set up Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def validate_bucket_exists(bucket_name: str, client: Minio) -> bool:
    """
    Validates whether a bucket exists in MinIO.

    Returns:
        bool: True if the bucket exists otherwise False.
    """
    try:
        # Check if the bucket exists
        if client.bucket_exists(bucket_name):
            return True
        else:
            
            return False
    except S3Error as e:
        # Log the error with details
        logger.error(f"Error while checking bucket '{bucket_name}': {e}")
        return False


def check_folder_exists(bucket_name: str, folder_name: str, client: Minio):
    """
    Checks if a specific folder exists in a given MinIO bucket.

    Returns:
        bool: True if the folder exists (i.e., objects with the given prefix exist), False otherwise.
    """    
    try:
        # Ensure folder_name has a trailing slash (for folders)
        if not folder_name.endswith('/'):
            folder_name += '/'

        # List objects with the given folder path
        objects = client.list_objects(bucket_name, prefix=folder_name, recursive=False)
        for obj in objects:
            # Checks if any object exists with the prefix
            if obj.object_name.startswith(folder_name):
                return True

        return False
    except S3Error as e:
        print(f"Error occurred: {e}")
        return False


def main():
    bucket_name = "prices"
    
    # List of folders to validate
    folder_name_list = ['daily_scrapper', 'markets', 'snifim']

    # import minio client from connections
    minio_client = init_minio_client()

    # Validate bucket
    bucket_validation = validate_bucket_exists(bucket_name, minio_client)
    if bucket_validation:
        logger.info(f"Bucket '{bucket_name}' exists.")
    else:
        logger.info(f"Bucket '{bucket_name}' does not exist.")
        # if bucket is not exists - stop execution
        sys.exit(1)


    # Validate all folders in the list
    for folder_name in folder_name_list:
        if not check_folder_exists(bucket_name, folder_name, minio_client):
            logger.warning(f"Folder '{folder_name}' does not exist in bucket '{bucket_name}'. Stopping execution.")
            #sys.exit(1)  # Exit if any folder does not exist
        else:
            logger.info(f"Folder '{folder_name}' exists in bucket '{bucket_name}'.")
    
    logger.info("All specified folders exist in the bucket.") 
        
            
if __name__ == '__main__':
    main()  
