import sys
import os
import logging
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from minio import Minio
import zipfile
import io

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import init_minio_client

# Load configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_objects_older_than(minio_client:Minio, bucket_name:str, threshold_days:int) -> list:
    """
    Find objects older than the specified threshold in a bucket.

    Returns:
        A list of objects that are older than the specified threshold.
    """
    # define trashold date (current date - threshold)
    threshold_date = datetime.now(timezone.utc) - timedelta(days=threshold_days)

    old_objects = []

    # itereates over all objects in the bucket.
    for obj in minio_client.list_objects(bucket_name, recursive=True):
        # append objects to old_objects if their last_modified date is older than the threshold_date
        if obj.last_modified < threshold_date:
            old_objects.append(obj)

    return old_objects


def update_zip_archive(minio_client:Minio, bucket_name:str, archive_name:str, old_objects:list):
    """
    Incrementally update or create a zip archive in the MinIO bucket.

    Returns:
        None
    """
    try:
        # Try to retrieve the existing archive
        archive_buffer = io.BytesIO()
        
        try:
            # Get data of an object in specified bucket/archive
            response = minio_client.get_object(bucket_name, archive_name)
            
            # Write content of the arcive to a buffer
            archive_buffer.write(response.read())
            
            # Close buffer
            response.close()
            response.release_conn()
        except Exception:
            logger.warning(f"Archive '{archive_name}' not found.")

        # Append files to the archive
        with zipfile.ZipFile(archive_buffer, mode="a", compression=zipfile.ZIP_DEFLATED) as zipf:
            for obj in old_objects:
                # Add ne file into the archive
                with minio_client.get_object(bucket_name, obj.object_name) as file_data:
                    zipf.writestr(obj.object_name, file_data.read())
                logger.info(f"Adding {obj.object_name} to archive.")

        # Upload the updated archive back to MinIO
        archive_buffer.seek(0)
        minio_client.put_object(
            bucket_name,
            archive_name,
            archive_buffer,
            length=archive_buffer.getbuffer().nbytes,
            content_type="application/zip"
        )
        logger.info(f"Successfully uploaded archive '{archive_name}' to bucket '{bucket_name}'.")

    except Exception as e:
        logger.error(f"Failed to update or create archive '{archive_name}': {e}")
    finally:
        archive_buffer.close()


def archive_and_delete_files(minio_client:Minio, bucket_name:str, threshold:int, archive_name:str):
    """
    Archive files older than the threshold and delete them from the bucket. 

    Returns:
        None
    """
    old_objects = get_objects_older_than(minio_client, bucket_name, threshold)
    # in case of no files older than a threshold - stop excution of the file
    if not old_objects:
        logger.info("No files to archive.")
        sys.exit(1)

    update_zip_archive(minio_client, bucket_name, archive_name, old_objects)
    
    # run over files in old_objects list and delete them from the bucket (already in the zip)
    for obj in old_objects:
        logger.info(f"Deleting original file: {obj.object_name}")
        minio_client.remove_object(bucket_name, obj.object_name)

def main():
    # MinIO client configuration
    minio_client = minio_client = init_minio_client()
    
    # Set parameters
    bucket_name = "prices"
    threshold = 7
    archive_name = "archive/archived_files.zip"

    archive_and_delete_files(minio_client, bucket_name, threshold, archive_name)

if __name__ == "__main__":
    main()
