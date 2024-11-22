# Standard Library Imports
import sys
import os
import logging
import time

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


excel_path = '/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/Files/markets_deatails_pub.xlsx'

df = pd.read_excel(excel_path)
df['file_date'] = datetime.now()
print(df)
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