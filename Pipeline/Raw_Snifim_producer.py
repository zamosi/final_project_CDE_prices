import sys
import os
import logging
from configparser import ConfigParser

from pyspark.sql import SparkSession

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import spark_read_data_from_postgres
from Connections.connection import write_to_kafka


# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)



# Main execution
if __name__ == "__main__":
    try:
        # Load configuration settings.
        spark_app_name = 'Postgres_Test'

        # Postgres source table name.
        table_name = "raw_data.snifim"

        # Path to postgres driver
        postgres_jdbc_driver_path = config["Core_Settings"]["POSTGRES_JDBC_DRIVERS_PATH"]

        # Kafka init:
        kafka_topic = 'Snifim'
        kafka_bootstrap_servers = config["Kafka"]["KAFKA_BOOTSTRAP_SERVERS"]

        # Initialize SparkSession
        try:
            spark = (
                SparkSession
                    .builder
                    .appName(spark_app_name)
                    .master("local[*]")
                    .config("spark.driver.memory", "4g") 
                    .config("spark.jars", postgres_jdbc_driver_path)
                    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0')
                    .getOrCreate()
            )
            
            logger.info(f"SparkSession for app: {spark_app_name} initialized successfully.")
        except Exception as e:
            logger.error("Failed to initialize SparkSession.", exc_info=True)
            raise e


       # Read data from a PostgreSQL table
        try:
            df = spark_read_data_from_postgres(spark, table_name)
            df.show()
        except Exception as e:
            logger.error(f"Failed to read data from PostgreSQL table: {table_name}.")
            raise e
        
        # Write data to Kafka
        try:
            write_to_kafka(df, kafka_bootstrap_servers, kafka_topic)
        except Exception as e:
            logger.error("Failed to produce data to Kafka.", exc_info=True)
            raise e

    except Exception as e:
        print(f"Error reading data from PostgreSQL: {e}")

    finally:
        spark.stop()
        logger.info("SparkSession stopped.")
