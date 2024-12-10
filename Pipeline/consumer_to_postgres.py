import sys
import os
import logging
from configparser import ConfigParser


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import  spark_consumer_to_df,spark_write_data_to_postgres

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

try:
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName('consumer_prices') \
        .config('spark.jars.packages', config["Kafka"]["KAFKA_JAR"]) \
        .config("spark.jars", config["Core_Settings"]["POSTGRES_JDBC_DRIVERS_PATH"])\
        .getOrCreate() 

    df = spark_consumer_to_df(spark,topic,prices_schema)
    spark_write_data_to_postgres(spark,'raw_data.prices_n',df)

except Exception as e:
    logger.error(e)
    sys.exit(1)