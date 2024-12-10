import sys
import os
import logging
from configparser import ConfigParser


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import  spark_consumer_to_df, spark_read_data_from_postgres, spark_write_data_to_bucket

# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    topic= 'prices'

    prices_schema = T.StructType([T.StructField('priceupdatedate',T.StringType(),True),
                                T.StructField('itemcode',T.StringType(),True),
                                T.StructField('itemtype',T.StringType(),True),
                                T.StructField('itemname',T.StringType(),True),
                                T.StructField('manufacturername',T.StringType(),True),
                                T.StructField('manufacturecountry',T.StringType(),True),
                                T.StructField('manufactureritemdescription',T.StringType(),True),
                                T.StructField('unitqty',T.StringType(),True),
                                T.StructField('quantity',T.StringType(),True),
                                T.StructField('unitofmeasure',T.StringType(),True),
                                T.StructField('bisweighted',T.StringType(),True),
                                T.StructField('qtyinpackage',T.StringType(),True),
                                T.StructField('itemprice',T.StringType(),True),
                                T.StructField('unitofmeasureprice',T.StringType(),True),
                                T.StructField('allowdiscount',T.StringType(),True),
                                T.StructField('itemstatus',T.StringType(),True),
                                T.StructField('itemid',T.StringType(),True),
                                T.StructField('file_name',T.StringType(),True),
                                T.StructField('num_reshet',T.StringType(),True),
                                T.StructField('num_snif',T.StringType(),True),
                                T.StructField('file_date',T.TimestampType(),True),
                                T.StructField('run_time',T.TimestampType(),True)
                                ])


    try:
        spark = SparkSession \
            .builder \
            .master("local") \
            .appName('consumer_prices') \
            .config('spark.jars.packages', config["Kafka"]["KAFKA_JAR"]) \
            .config("spark.jars", config["Core_Settings"]["POSTGRES_JDBC_DRIVERS_PATH"])\
            .getOrCreate() 
        
        # Set MinIO credentials and endpoint
        spark.conf.set("fs.s3a.endpoint", config["Minio"]["HTTP_Url"])
        spark.conf.set("fs.s3a.access.key", config["Minio"]["Access_Key"])
        spark.conf.set("fs.s3a.secret.key", config["Minio"]["Secret_Key"])
        spark.conf.set("fs.s3a.path.style.access", "true")
        
        logger.info('Spark session created')

        # read stream
        df = spark_consumer_to_df(spark,topic,prices_schema)

        # Import reshatot table from postgres     
        df_reshatot = spark_read_data_from_postgres(spark, 'raw_data.reshatot')

        # Broadcast the smaller DataFrame (df_reshatot)
        broadcast_reshatot = F.broadcast(df_reshatot).select("reshet_num", "reshet_name")

        # Add snapshot column based on run-date column
        df = df.withColumn("snapshot", F.col("run_time").cast(T.DateType()))

        # Create snapshot_month and snapshot_quarter columns
        df = df\
            .withColumn("snapshot_month", F.date_format(F.col("run_time"), "MMMM")) \
            .withColumn("snapshot_quarter", F.concat_ws(" - ", F.date_format(F.col("run_time"), "yyyy"), F.quarter(F.col("run_time"))))

        # Create is_weekend column based on day of the week
        df = df.withColumn("is_weekend", 
                        F.when(F.date_format(F.col("run_time"), "EEEE").isin( "Friday", "Saturday"), "Weekend")
                        .otherwise("Weekday"))

        selected_columns = [
            "snapshot", "snapshot_month", "snapshot_quarter", "is_weekend", 
            "itemcode", "itemname", "manufacturername", "manufacturecountry", 
            "manufactureritemdescription", "unitqty", "unitofmeasure", 
            "qtyinpackage", "itemprice", "unitofmeasureprice", "allowdiscount", 
            "itemid", "file_name", "num_reshet", "priceupdatedate", "file_date", "run_time"
        ]

        # Filter selected columns
        df = df.select(selected_columns)

        # Broadcast join with the reshatot table
        df = df.join(broadcast_reshatot, df.num_reshet == broadcast_reshatot.reshet_num, "left_outer")

        # Add reshet_name from the reshatot table
        df = df.withColumn("reshet_name", F.col("reshet_name"))

        # Add the 'days_since_last_price_update' column
        df = df.withColumn("days_since_last_price_update", F.datediff(F.col("file_date"), F.col("priceupdatedate")))

        # Add the 'days_since_last_price_update' column
        df = df.withColumn("days_since_last_price_update", F.datediff(F.col("file_date"), F.col("priceupdatedate")))

        # Add the 'is_price_update_stale' column based on days_since_last_price_update > 60
        df = df.withColumn(
            "is_price_update_stale", 
            F.when(F.col("days_since_last_price_update") > 60, 1).otherwise(0)
        )        

        # Drop duplicate colum from enriched df
        df = df.drop("reshet_num") 

        # Write Parquet file to the specified bucket
        spark_write_data_to_bucket(df, 'testbucket')

        spark.stop()


    except Exception as e:
        logger.error(e)
        sys.exit(1)   


if __name__ == '__main__':
    main();        