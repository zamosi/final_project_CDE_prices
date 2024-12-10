import sys
import os
import logging
from configparser import ConfigParser


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import procuder_minio_to_kafka

# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Defines the schema of prices / snifim df.
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

snifim_schema = T.StructType([T.StructField('storeid',T.StringType(),True),
                            T.StructField('bikoretno',T.StringType(),True),
                            T.StructField('storetype',T.StringType(),True),
                            T.StructField('storename',T.StringType(),True),
                            T.StructField('address',T.StringType(),True),
                            T.StructField('city',T.StringType(),True),
                            T.StructField('zipcode',T.StringType(),True),
                            T.StructField('file_name',T.StringType(),True),
                            T.StructField('num_reshet',T.StringType(),True),
                            T.StructField('file_date',T.TimestampType(),True),
                            T.StructField('run_time',T.TimestampType(),True)
                            ])
try:
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName('procuder_minio_to_kafka') \
        .config('spark.jars.packages', config["Kafka"]["KAFKA_JAR"]) \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    logger.info("Spark session created.")

    # Listens to new files in Minio Bucket and sents it into kafka
    procuder_minio_to_kafka(spark,'snifim', prices_schema)

except Exception as e:
    logging.error(e)
    sys.exit(1)    





















