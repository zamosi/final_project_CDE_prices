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


def main():
    bucket_name = "prices"
    
    minio_client = init_minio_client()

    # Validate bucket
    bucket_validation = validate_bucket_exists(bucket_name, minio_client)

    if bucket_validation:
        logger.info(f"Bucket '{bucket_name}' exists.")
    else:
        logger.info(f"Bucket '{bucket_name}' does not exist.")
        sys.exit(1)
        
            
if __name__ == '__main__':
    main()  
