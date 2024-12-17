import sys
import os
import logging
from configparser import ConfigParser

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import(spark_consumer_to_df, 
                                   spark_read_data_from_postgres, 
                                   spark_write_data_to_bucket, 
                                   spark_read_from_bucket, 
                                   spark_write_data_to_postgres,
                                   save_offsets)

# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Suppress Kafka's internal logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    topic = 'prices'
    path_to_snifim_bucket = 's3a://snifim/'

    prices_schema = T.StructType([
        T.StructField('priceupdatedate', T.StringType(), True),
        T.StructField('itemcode', T.StringType(), True),
        T.StructField('itemtype', T.StringType(), True),
        T.StructField('itemname', T.StringType(), True),
        T.StructField('manufacturername', T.StringType(), True),
        T.StructField('manufacturecountry', T.StringType(), True),
        T.StructField('manufactureritemdescription', T.StringType(), True),
        T.StructField('unitqty', T.StringType(), True),
        T.StructField('quantity', T.StringType(), True),
        T.StructField('unitofmeasure', T.StringType(), True),
        T.StructField('bisweighted', T.StringType(), True),
        T.StructField('qtyinpackage', T.StringType(), True),
        T.StructField('itemprice', T.StringType(), True),
        T.StructField('unitofmeasureprice', T.StringType(), True),
        T.StructField('allowdiscount', T.StringType(), True),
        T.StructField('itemstatus', T.StringType(), True),
        T.StructField('file_name', T.StringType(), True),
        T.StructField('num_reshet', T.StringType(), True),
        T.StructField('num_snif', T.StringType(), True),
        T.StructField('file_date', T.TimestampType(), True),
        T.StructField('run_time', T.TimestampType(), True)
    ])

    try:
        # Initialize Spark session
        spark = SparkSession \
            .builder \
            .master("local") \
            .appName('consumer_prices') \
            .config('spark.jars.packages', config["Kafka"]["KAFKA_JAR"]) \
            .config("spark.jars", config["Core_Settings"]["POSTGRES_JDBC_DRIVERS_PATH"]) \
            .config("fs.s3a.endpoint", config["Minio"]["HTTP_Url"]) \
            .config("fs.s3a.access.key", config["Minio"]["Access_Key"]) \
            .config("fs.s3a.secret.key", config["Minio"]["Secret_Key"]) \
            .config("fs.s3a.path.style.access", "true") \
            .getOrCreate()
        
        logger.info('Spark session created.')

        # Consumer group
        consumer_group = config["Kafka"]["PRICES_CONSUMER_GROUP"]
        
        # Path to offset files
        offset_files = config["Kafka"]["PRICES_OFFSETS_FILE"]

        # Consume Kafka stream with offsets
        df_with_metadata = spark_consumer_to_df(spark, topic, prices_schema, offset_files, consumer_group)

        # Extract offsets immediately for saving
        offsets_df = df_with_metadata.select("topic", "partition", "offset").distinct()

        # Drop metadata columns to prevent further errors
        df = df_with_metadata.drop("topic", "partition", "offset")

        # Check if DataFrame has rows
        if df.count() > 1:
            logger.info("Processing data...")

            # Import reshatot table from Postgres
            df_reshatot = spark_read_data_from_postgres(spark, 'raw_data.reshatot')
            df_reshatot = df_reshatot.select("reshet_num", "reshet_name")  # Select only required columns

            # Import snifim data from MinIO bucket
            df_snifim = spark_read_from_bucket(spark, path_to_snifim_bucket, "parquet").dropDuplicates()
            df_snifim = df_snifim.selectExpr(
                "storeid as snif_storeid", 
                "num_reshet as snif_reshet_num", 
                "storename", "address", "city", "zipcode"
            )  # Rename conflicting columns

            df_snifim = df_snifim.dropDuplicates()

            # Add calculated columns
            df = df.withColumn("snapshot", F.col("run_time").cast(T.DateType())) \
                .withColumn("snapshot_month", F.date_format(F.col("run_time"), "MMMM")) \
                .withColumn("snapshot_quarter", F.concat_ws(" - ", F.date_format(F.col("run_time"), "yyyy"), F.quarter(F.col("run_time")))) \
                .withColumn("is_weekend", F.when(F.date_format(F.col("file_date"), "EEEE").isin("Friday", "Saturday"), True).otherwise(False))
                

            
            # Select required columns
            selected_columns = [
                "snapshot", "snapshot_month", "snapshot_quarter", "is_weekend",
                "itemcode", "itemname", "manufacturername", "manufacturecountry",
                "manufactureritemdescription", "unitqty", "unitofmeasure", 
                "qtyinpackage", "itemprice", "unitofmeasureprice", "allowdiscount", 
                "file_name", "num_reshet", "priceupdatedate", "file_date", "run_time", 
                "num_snif"
            ]

            df = df.select(selected_columns)

            # Cast to integer
            df = df.withColumn("num_snif", F.col("num_snif").cast(T.IntegerType()))
            df_snifim = df_snifim.withColumn("snif_storeid", F.col("snif_storeid").cast(T.IntegerType()))


            # Join with reshatot and snifim tables
            df = df.join(F.broadcast(df_reshatot), df.num_reshet == df_reshatot.reshet_num, "left_outer") \
                   .drop("reshet_num")

            df = df.join(F.broadcast(df_snifim), (df.num_reshet == df_snifim.snif_reshet_num) &\
                         (df.num_snif == df_snifim.snif_storeid), "left_outer") \
                   .drop("snif_storeid", "snif_reshet_num")

            # Add derived columns
            df = df.withColumn("days_since_last_price_update", F.datediff(F.col("file_date"), F.col("priceupdatedate"))) \
                   .withColumn("is_price_update_stale", F.when(F.col("days_since_last_price_update") > 60, 1).otherwise(0))
            
            # Add default values in the final DataFrame
            df = df\
                .withColumn("zipcode", F.when(F.col("zipcode").isNull(), -999).otherwise(F.col("zipcode"))) \
                .withColumn("city", F.when(F.col("city").isNull(), "Unknown").otherwise(F.col("city")))\
                .withColumn("reshet_name", F.when(F.col("reshet_name").isNull(), "Unknown").otherwise(F.col("reshet_name")))\
                .withColumn("storename", F.when(F.col("storename").isNull(), "Unknown").otherwise(F.col("storename")))\
                .withColumn("address", F.when(F.col("address").isNull(), "Unknown").otherwise(F.col("address")))\
                .withColumn("zipcode", F.when(F.col("zipcode") == "unknown", -999).otherwise(F.col("zipcode")))
            
            # Casting of columns:
            df = df\
                .withColumn("allowdiscount", F.col("allowdiscount").cast("boolean"))\
                .withColumn("itemcode", F.col("itemcode").cast("bigint"))\
                .withColumn("itemprice", F.col("itemprice").cast("float"))\
                .withColumn("num_reshet", F.col("num_reshet").cast("bigint"))\
                .withColumn("priceupdatedate", F.col("priceupdatedate").cast("timestamp"))\
                .withColumn("unitofmeasureprice", F.col("unitofmeasureprice").cast("float"))\
                .withColumn("zipcode", F.col("zipcode").cast("integer"))
            
            logger.info("Replaced null values with default values...")

            # Write to MinIO bucket
            spark_write_data_to_bucket(df, 'ml-bucket')
            logger.info("Data successfully written to MinIO bucket.")

            # Write to Postgres db
            dest_postgers_datble = "dwh.prices_data"
            spark_write_data_to_postgres(spark, dest_postgers_datble, df)
            logger.info(f"Data successfully written to {dest_postgers_datble} table.")


            save_offsets(offsets_df, config["Kafka"]["PRICES_OFFSETS_FILE"])
            logger.info("Offsets saved successfully.")
        else:
            logger.info("DataFrame has 1 or fewer rows. Skipping processing.")

        # Stop Spark session
        logger.info("Stopping Spark session...")
        spark.stop()

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
