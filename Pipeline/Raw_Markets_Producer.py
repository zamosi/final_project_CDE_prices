import sys
import os
import logging
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
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
        # Load configuration settings
        spark_app_name = 'Markets_Producer'

        # Number of rows per bulk set to Kafka
        chunk_size = 1000

        # Source file path
        file_path = f'{config["Core_Settings"]["SOURCE_FOLDER_MARKETS"]}/markets.parquet'

        # Kafka configuration
        kafka_topic = 'Raw_Markets'
        kafka_bootstrap_servers = config["Kafka"]["KAFKA_BOOTSTRAP_SERVERS"]

        # Initialize SparkSession
        try:
            spark = (
                SparkSession
                .builder
                .appName(spark_app_name)
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0')
                .getOrCreate()
            )
            logger.info(f"SparkSession for app: {spark_app_name} initialized successfully.")
        except Exception as e:
            logger.error("Failed to initialize SparkSession.", exc_info=True)
            raise e

        # Write data to Kafka
        try:
            # Read the entire Parquet file using Spark
            df = spark.read.parquet(file_path)

            # Normalize datetime columns in the Spark DataFrame
            datetime_columns = [field.name for field in df.schema.fields if str(field.dataType) == 'TimestampType']
            for col_name in datetime_columns:
                df = df.withColumn(col_name, to_timestamp(col(col_name)))

            # write to Kafka
            write_to_kafka(df, kafka_bootstrap_servers, kafka_topic)
            logger.info(f"Data successfully written to Kafka topic: {kafka_topic}")

        except Exception as e:
            logger.error("Failed to produce data to Kafka.", exc_info=True)
            raise e

    except Exception as e:
        logger.error("An error occurred during execution.", exc_info=True)

    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("SparkSession stopped.")
