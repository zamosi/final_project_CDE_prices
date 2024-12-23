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


def get_objects_older_than(minio_client: Minio, bucket_name: str, threshold_days: int, archive_name: str) -> list:
    """
    Find objects older than the specified threshold in a bucket, excluding the archive itself.
    """
    threshold_date = datetime.now(timezone.utc) - timedelta(days=threshold_days)
    old_objects = []

    for obj in minio_client.list_objects(bucket_name, recursive=True):
        if obj.last_modified < threshold_date and obj.object_name != archive_name:
            old_objects.append(obj)

    return old_objects



def update_zip_archive(minio_client: Minio, bucket_name: str, archive_name: str, old_objects: list):
    """
    Incrementally update or create a zip archive in the MinIO bucket.
    """
    archive_buffer = io.BytesIO()
    is_new_archive = False

    try:
        # Try to retrieve the existing archive in streaming mode
        try:
            response = minio_client.get_object(bucket_name, archive_name)
            try:
                with zipfile.ZipFile(response, mode="r") as existing_archive:
                    # Copy existing files to the new archive buffer
                    with zipfile.ZipFile(archive_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as new_archive:
                        for item in existing_archive.infolist():
                            new_archive.writestr(item, existing_archive.read(item.filename))
            finally:
                response.close()
                response.release_conn()
        except Exception:
            logger.warning(f"Archive '{archive_name}' not found. Creating a new archive.")
            is_new_archive = True

        # Append new files to the archive buffer
        with zipfile.ZipFile(archive_buffer, mode="a", compression=zipfile.ZIP_DEFLATED) as zipf:
            for obj in old_objects:
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

        if is_new_archive:
            logger.info(f"Created new archive '{archive_name}' in bucket '{bucket_name}'.")
        else:
            logger.info(f"Updated archive '{archive_name}' in bucket '{bucket_name}'.")

    except Exception as e:
        logger.error(f"Failed to update or create archive '{archive_name}': {e}")
    finally:
        archive_buffer.close()



def archive_and_delete_files(minio_client: Minio, bucket_name: str, threshold: int, archive_name: str):
    """
    Archive files older than the threshold and delete them from the bucket.
    """
    old_objects = get_objects_older_than(minio_client, bucket_name, threshold, archive_name)

    if not old_objects:
        logger.warning("No files to archive.")
        return  # Exit early if there are no files to process

    update_zip_archive(minio_client, bucket_name, archive_name, old_objects)

    # Delete old files (excluding the archive itself)
    for obj in old_objects:
        logger.info(f"Deleting original file: {obj.object_name}")
        minio_client.remove_object(bucket_name, obj.object_name)

def main():
    try:
        # MinIO client configuration
        minio_client = init_minio_client()
        logger.info("MinIO client initialized successfully.")

        # Set parameters
        bucket_name = "prices"
        threshold = 0  # Adjust threshold for debugging
        archive_name = "archive/archived_files.zip"

        logger.info(f"Starting archiving process for bucket '{bucket_name}' with threshold {threshold} days.")
        archive_and_delete_files(minio_client, bucket_name, threshold, archive_name)
        logger.info("Archiving process completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
