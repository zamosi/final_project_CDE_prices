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
        logger.info(f"Data inserted successfully into {table_name} table.")
    except Exception as e:
        logger.error(f"Error inserting data into PostgreSQL: {e}")


def check_file_exists(path:str) -> bool:
    """
    Validates if a file exists at the given path.
    
    Returns:
        bool: True if the file exists, False otherwise.
    """
    return os.path.isfile(path)


def main():
    # Define path to Excel with stores
    excel_path = config["Core_Settings"]["MARKETS_DETAILS_EXCEL_PATH"]

    conn, engine = connect_to_postgres_data()

    # Validate file existence
    if check_file_exists(excel_path):
        df = pd.read_excel(excel_path)
        df['run_time'] = datetime.now()
        df['password'] = df['password'].astype(str).str.strip()
        insert_dataframe_to_postgres(engine, df, 'reshatot')
    else:
        logger.critical("File doesn't exist in the given path, please set the file first in the folder!")

    conn.close()
    logger.info("Connection closed.")
    
    engine.dispose()
    logger.info("Engine disposed.")


if __name__ == '__main__':
    main()