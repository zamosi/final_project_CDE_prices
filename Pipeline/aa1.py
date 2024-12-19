import sys
import os
import logging
from configparser import ConfigParser


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import  spark_consumer_to_df,spark_write_data_to_postgres,spark_read_data_from_postgres
from Connections.connection import connect_to_postgres_data,truncate_table_in_postgres



# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)




spark = SparkSession \
.builder \
.master("local") \
.appName('consumer_prices') \
.config("spark.executor.memory", "8g") \
.config("spark.driver.memory", "4g") \
.config("spark.sql.shuffle.partitions", "200") \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
.config("spark.jars", config["Core_Settings"]["POSTGRES_JDBC_DRIVERS_PATH"])\
.getOrCreate()


try:
    df_old = spark_read_data_from_postgres(spark, 'dwh.prices_scd')
    logger.info("load table dwh.prices_scd from postgres")
except Exception as e:
    logger.error(f"table not load {e}")
    raise

df_old.show()

spark.stop()