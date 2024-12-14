import logging
from datetime import datetime

from psycopg2 import connect
from configparser import ConfigParser
from sqlalchemy import create_engine

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_json, struct
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import functions as F
from pyspark.sql import types as T

from minio import Minio


# Read database configuration from the config file
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


#****************************************************************************************************
#*                                            Postgres                                              *
#****************************************************************************************************

def connect_to_postgres_data():
    """
    Connects to a PostgreSQL database  based on values parsed from config file and returns a connection 
    object and SQLAlchemy engine.

    Returns:
        tuple: A tuple containing the psycopg2 connection object and SQLAlchemy engine.
               Returns None if an error occurs.
    """    
    try:
        # Connect to PostgreSQL using psycopg2-binary
        conn = connect(
            host=config["Postgres_Data"]["HOST"],
            port=config["Postgres_Data"]["PORT"],
            user=config["Postgres_Data"]["USER"],
            password=config["Postgres_Data"]["PASSWORD"],
            database=config["Postgres_Data"]["DB"]
        )
        logger.info("Connected to PostgreSQL database successfully.")

        conn_str = f"postgresql+psycopg2://{config['Postgres_Data']['USER']}:{config['Postgres_Data']['PASSWORD']}@{config['Postgres_Data']['HOST']}:{config['Postgres_Data']['PORT']}/{config['Postgres_Data']['DB']}"
        engine = create_engine(conn_str)
        # Return connection object for further use
        return conn, engine

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None

def truncate_table_in_postgres(conn,table_name):

    try:
        cursor = conn.cursor()
        truncate_query = f"TRUNCATE TABLE {table_name}"
        cursor.execute(truncate_query)
        conn.commit() 
        logger.info(f"Table {table_name} truncated successfully.")
    except Exception as e:
        logger.error(f"Error occurred while truncating table: {e}")
    finally:
        cursor.close()

def spark_read_data_from_postgres(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Read data from a PostgreSQL table into a PySpark DataFrame.

    Returns:
        DataFrame: PySpark DataFrame containing the table data.
    """
    # JDBC connection string with UTF-8 encoding
    jdbc_url = f"jdbc:postgresql://{config['Postgres_Data']['HOST']}:{config['Postgres_Data']['PORT']}/{config['Postgres_Data']['DB']}?stringtype=unspecified&characterEncoding=UTF-8"
    
    database_properties = {
        "user": config["Postgres_Data"]["USER"],
        "password": config["Postgres_Data"]["PASSWORD"],
        "driver": "org.postgresql.Driver"
    }

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", database_properties["user"]) \
        .option("password", database_properties["password"]) \
        .option("driver", database_properties["driver"]) \
        .option("characterEncoding", "UTF-8") \
        .load()

    return df

def spark_write_data_to_postgres(spark: SparkSession, table_name: str,df):

    jdbc_url = f"jdbc:postgresql://{config['Postgres_Data']['HOST']}:{config['Postgres_Data']['PORT']}/{config['Postgres_Data']['DB']}"

    postgres_options  = {
        "user": config["Postgres_Data"]["USER"],
        "password": config["Postgres_Data"]["PASSWORD"],
        "driver": "org.postgresql.Driver",
        "dbtable":table_name,
        "url": jdbc_url
        }

    df.write \
        .jdbc(url=postgres_options["url"], table=postgres_options["dbtable"], mode="append", properties=postgres_options) 


#****************************************************************************************************
#*                                              Kafka                                               *
#****************************************************************************************************

def spark_consumer_to_df(spark: SparkSession,topic:str,schema):
    stream_df = spark \
        .read \
        .format('kafka') \
        .option("kafka.bootstrap.servers", config["Kafka"]["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("subscribe", topic) \
        .option("kafka.group.id", "myConsumerGroup")\
        .load() \
        .select(F.col('value').cast(T.StringType()))

    parsed_df = stream_df \
        .withColumn('parsed_json', F.from_json(F.col('value'), schema)) \
        .select(F.col('parsed_json.*'))
    

    return parsed_df 


def procuder_minio_to_kafka(spark:SparkSession,topic,schema):
    # Set MinIO credentials and endpoint
    spark.conf.set("fs.s3a.endpoint", config["Minio"]["HTTP_Url"])
    spark.conf.set("fs.s3a.access.key", config["Minio"]["Access_Key"])
    spark.conf.set("fs.s3a.secret.key", config["Minio"]["Secret_Key"])
    spark.conf.set("fs.s3a.path.style.access", "true")

    df_stream = spark.read \
        .format("parquet") \
        .schema(schema)\
        .option("kafka.group.id", "myConsumerGroup")\
        .option("path", f"s3a://{topic}/") \
        .load()
    
    logger.info("spark read-stream created.")

    json_df = df_stream.select(F.to_json(F.struct([col for col in df_stream.columns])).alias("value"))


    query = json_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config["Kafka"]["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("topic", topic) \
        .save()


    logger.info("Transfer data to kafka - completed.")


#****************************************************************************************************
#*                                              MinIO                                               *
#****************************************************************************************************    


def init_minio_client() -> Minio:
    """
    Initializes and returns a MinIO client instance.

    Returns:
        Minio: An instance of the Minio client.
    """    
    try:
        minio_client = Minio(
            config["Minio"]["Url"],
            access_key=config["Minio"]["Access_Key"],
            secret_key=config["Minio"]["Secret_Key"],
            secure=False
        )

        return minio_client
    
    except Exception as e:
        logger.error(f'Unable create MinIO client with the following error: \n{e}')
        raise
    

def spark_write_data_to_bucket(df: DataFrame, bucket_name: str):
    """
    Writes the DataFrame as Parquet files to the MinIO bucket with file naming convention ml_yyyymmdd.
    """
    try:
        # Generate the dynamic output path with the desired file name format
        date_str = datetime.now().strftime("%Y%m%d")
        output_path = f"s3a://{bucket_name}/{date_str}/"  # Updated to include ml_yyyymmdd in the path

        df.write \
            .format("parquet") \
            .mode("append") \
            .option("path", output_path) \
            .save()

        logger.info(f"Data written successfully to {output_path}")

    except Exception as e:
        logger.error(f"Error writing Parquet files to bucket: {e}")
        raise