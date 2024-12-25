import logging
from datetime import datetime
import sys
import json

from psycopg2 import connect
from configparser import ConfigParser
from sqlalchemy import create_engine

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType

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
    Connects to a PostgreSQL database  based on values parsed from config file, returns a connection 
    object and SQLAlchemy engine.

    Returns:
        A tuple containing the psycopg2 connection object and SQLAlchemy engine.
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
    """
    Writes a Spark DataFrame to a PostgreSQL database table.
    """
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
def load_offsets(path:str) -> dict:
    """
    Load the last stored offsets from a file. If not found or invalid, return 'earliest'.
    """
    try:
        with open(path, 'r') as file:
            content = file.read()
             # Check if file is blank
            if not content.strip(): 
                logger.warning("Offset file is blank. Using 'earliest'.")
                return {"offsets": "earliest"}
            
            offsets = json.loads(content)
            if not isinstance(offsets, dict) or "offsets" not in offsets:
                logger.warning("Offset file is invalid. Using 'earliest'.")
                return {"offsets": "earliest"}

            return offsets
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Offset file error: {e}. Using 'earliest'.")
        return {"offsets": "earliest"}


def save_offsets(df: DataFrame, path:str):
    """
    Save the latest offsets after processing.
    """
    # Extract Kafka offsets metadata
    offsets = df.select("topic", "partition", "offset") \
                .groupBy("topic", "partition") \
                .agg(F.max("offset").alias("latest_offset")) \
                .collect()

    # Structure offsets into JSON format
    offset_data = {"offsets": {}}
    for row in offsets:
        topic = row['topic']
        partition = row['partition']
        offset = row['latest_offset']
        offset_data["offsets"][f"{topic}-{partition}"] = offset

    # Save to file
    with open(path, 'w') as file:
        json.dump(offset_data, file)
    logger.info("Offsets successfully saved.")


def spark_consumer_to_df(spark: SparkSession, topic: str, schema: StructType, offset_file:str, consumer_group:str) -> DataFrame:
    """
    Reads data from a Kafka topic, starting from the last stored offsets, and returns a DataFrame with message data 
    and metadata for offset tracking.

    Returns:
    A Spark DataFrame containing parsed Kafka message data along with Kafka metadata:
        - Parsed fields from the schema.
        - Kafka metadata columns: `topic`, `partition`, `offset`.
    """

    # Load last stored offsets
    offsets = load_offsets(offset_file)
    
    # Prepare Kafka startingOffsets JSON
    starting_offsets = "earliest" 
    
    if "offsets" in offsets and isinstance(offsets["offsets"], dict):
        stored_offsets = offsets["offsets"]
        offsets_json = {topic: {int(key.split('-')[1]): offset for key, offset in stored_offsets.items()}}
        starting_offsets = json.dumps(offsets_json)

    # Read Kafka stream
    raw_df = spark \
        .read \
        .format('kafka') \
        .option("kafka.bootstrap.servers", config["Kafka"]["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .option("kafka.group.id", consumer_group)\
        .load()

    # Extract metadata and parsed JSON
    df_with_metadata = raw_df \
        .select(
            F.col("value").cast(T.StringType()),
            F.col("topic"),
            F.col("partition"),
            F.col("offset")
        ) \
        .withColumn('parsed_json', F.from_json(F.col('value'), schema)) \
        .select(
            F.col("parsed_json.*"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset")
        )

    return df_with_metadata


def procuder_minio_to_kafka(spark:SparkSession, topic:str, schema:StructType):
    # Set MinIO credentials and endpoint
    spark.conf.set("fs.s3a.endpoint", config["Minio"]["HTTP_Url"])
    spark.conf.set("fs.s3a.access.key", config["Minio"]["Access_Key"])
    spark.conf.set("fs.s3a.secret.key", config["Minio"]["Secret_Key"])
    spark.conf.set("fs.s3a.path.style.access", "true")

    df = spark.read \
        .format("parquet") \
        .schema(schema)\
        .option("kafka.group.id", "myConsumerGroup")\
        .option("path", f"s3a://{topic}") \
        .load()
    
    logger.info("spark read created.")

    json_df = df.select(F.to_json(F.struct([col for col in df.columns])).alias("value"))


    query = json_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config["Kafka"]["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("topic", topic) \
        .save()

    logger.info("Transfer data to kafka - completed.")


def procuder_to_kafka(spark:SparkSession,topic:str, df):
 

    spark_df = spark.createDataFrame(df)
    logger.info("convert DF to spark")

    json_df = spark_df.select(F.to_json(F.struct([col for col in spark_df.columns])).alias("value"))
    
    json_df \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config["Kafka"]["KAFKA_BOOTSTRAP_SERVERS"])\
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
    

def spark_read_from_bucket(spark: SparkSession, path: str, format: str = "parquet") -> DataFrame:
    """
    Reads data from a given bucket path into a Spark DataFrame.

    Returns:
        DataFrame: A Spark DataFrame containing the data from the bucket.
    """           
    
    try:
        df = spark.read.format(format).load(path)
        logger.info(f"Data from {path} loaded successfully.")
        return df
    
    except Exception as e:
        logger.error(e)
        sys.exit(1)


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