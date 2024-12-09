import sys
import os
import logging
from configparser import ConfigParser


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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
 .appName('consumer_1') \
 .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
 .getOrCreate()

# df = spark.read.parquet("s3a://prices/Price7290058140886-001-202412040800.parquet")
# df.printSchema()

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


df=spark.readStream \
    .format("parquet") \
    .schema(prices_schema)\
    .load("s3a://prices/*.parquet") 

df_with_kafka_format = df.selectExpr("to_json(struct(*)) AS value")


query = df_with_kafka_format.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "prices") \
    .option("checkpointLocation", "s3a://spark/checkpoints/prices") \
    .outputMode("append") \
    .start()

query.awaitTermination()




spark.stop()













