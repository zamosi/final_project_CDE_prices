import sys
import os
import logging
from configparser import ConfigParser
from datetime import datetime, timedelta

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import connect_to_postgres_data

# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def delete_old_data(conn, table_name:str, threshold: int):
    """
    Deletes rows older than a specified threshold (in days) from a given table in a PostgreSQL database.

    Returns:
        None
    """
    try:
        cursor = conn.cursor()
        
        # SQL query to delete rows older than 7 days
        delete_query = f"""
            DELETE FROM {table_name}
            WHERE snapshot <= CURRENT_DATE - INTERVAL '{threshold} days';
        """
        
        # Execute the query
        cursor.execute(delete_query)
        logger.info("cursor executted")

        # Commit the transaction
        conn.commit()
        logger.info("cursor comitted")

        # Get the number of deleted rows
        logger.info(f"{cursor.rowcount} rows deleted.")

        if cursor.rowcount > 0:
            logger.info("Old data cleanup completed successfully.")
        else:
            logger.info("There was no data older than 7 days")

    
    except Exception as e:
        logging.error(f"Error: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()


if __name__ == '__main__':

    # Set table name
    table_name = "dwh.prices_data"

    # Establish connection to the database
    conn, eng = connect_to_postgres_data()
    logger.info("Database connection established.")
    threshold = 7

    try:
        delete_old_data(conn, table_name, threshold)
    except Exception as e:
        logger.error(e)

    logger.info("cursor executted") if conn else None
    eng.dispose() if eng else None
