# Standard Library Imports
import sys
import os
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


def check_file_exists(path:str) -> bool:
    """
    Validates if a file exists at the given path.
    
    Parameters:
    file_path (str): The path of the file to validate.
    
    Returns:
    bool: True if the file exists, False otherwise.
    """
    return os.path.isfile(path)

def main():
    
    excel_path = config["Core_Settings"]["MARKETS_DETAILS_EXCEL_PATH"]

    if check_file_exists(excel_path):
        df = pd.read_excel(excel_path)
        df['run_time'] = datetime.now()

        #print(df)
        conn, engine = connect_to_postgres_data()
        try:
            # Insert DataFrame into PostgreSQL
            df.to_sql('reshatot', con=engine, schema='raw_data', if_exists='replace', index=False)
            logger.info(f"Data successfully inserted into table 'RESHATOT' in schema 'raw_data'.")
        except Exception as e:
            logger.error(f"Error inserting data into PostgreSQL: {e}", exc_info=True)
        finally:
            # Close the connection
            conn.close()
            logger.info("Database connection closed.")
            engine.dispose()
    else:
        logger.critical("File don't exits in the given path, please set the file first in the folder!")


if __name__ == '__main__':
    main()