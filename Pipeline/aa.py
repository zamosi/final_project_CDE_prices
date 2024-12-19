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
from Connections.connection import connect_to_postgres_data,truncate_table_in_postgres,save_offsets



# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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

spark = SparkSession \
.builder \
.master("local[*]") \
.appName('consumer_prices') \
.config("spark.executor.memory", "4g") \
.config("spark.driver.memory", "4g") \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
.config("spark.jars", config["Core_Settings"]["POSTGRES_JDBC_DRIVERS_PATH"])\
.getOrCreate()




# Consumer group
consumer_group = config["Kafka"]["scd_CONSUMER_GROUP"]
# Path to offset files
offset_files = config["Kafka"]["SCD_OFFSETS_FILE"]

#load new data from kafka

try:
    # Consume Kafka stream with offsets
    df_with_metadata = spark_consumer_to_df(spark, topic, prices_schema, offset_files, consumer_group)
    # Extract offsets immediately for saving
    offsets_df = df_with_metadata.select("topic", "partition", "offset").distinct()
    save_offsets(offsets_df, offset_files)
    # Drop metadata columns to prevent further errors
    df_new = df_with_metadata.drop("topic", "partition", "offset")
    
    logger.info("consumer load data to df")
except Exception as e:
    logger.error(f"consumer not load {e}")
    raise


#fiter just files "PriceFull"
df_new_full = df_new.filter(F.col("file_name").startswith("PriceFull")& (F.col('num_reshet') == '7290700100008')) \
                    .withColumn("file_date2",F.to_date(F.col("file_date")))

#add row_number column that give item per reshet,snif,day
window_spec = Window.partitionBy("itemcode", "num_reshet","num_snif","file_date2").orderBy(F.col("file_date").desc())
df_with_row_number = df_new_full.withColumn("rn", F.row_number().over(window_spec))


df_wo_duplicates_per_day_conv = df_with_row_number.select(
    F.col('itemcode').cast('bigint')\
    ,F.col('itemname').cast('string')\
    ,F.col('itemprice').cast('float')\
    ,F.col('file_date2').cast('date').alias('file_date')\
    ,F.col('num_reshet').cast('bigint')\
    ,F.col('num_snif').cast('int')\
    ,F.col('rn').cast('int')
        )



spark_write_data_to_postgres(spark,'dwh.all',df_wo_duplicates_per_day_conv)


spark.stop()































