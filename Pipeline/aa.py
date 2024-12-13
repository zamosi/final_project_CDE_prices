import sys
import os
import logging
from configparser import ConfigParser
from psycopg2 import connect
from sqlalchemy import create_engine
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import connect_to_postgres_data,truncate_table_in_postgres

# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

conn, engine = connect_to_postgres_data()


truncate_table_in_postgres(conn,'dwh.prices_scd')