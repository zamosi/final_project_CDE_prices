import logging

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
from minio.error import S3Error



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


def spark_read_data_from_postgres(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Read data from a PostgreSQL table into a PySpark DataFrame.

    Args:
        spark (SparkSession): SparkSession object.
        jdbc_url (str): JDBC URL for the PostgreSQL database.
        database_properties (dict): Dictionary with user, password, and driver info.
        table_name (str): Name of the PostgreSQL table to read.

    Returns:
        DataFrame: PySpark DataFrame containing the table data.
    """
    # JDBC connection string
    jdbc_url = f"jdbc:postgresql://{config['Postgres_Data']['HOST']}:{config['Postgres_Data']['PORT']}/{config['Postgres_Data']['DB']}"
    
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

    query = df.writeStream \
        .foreachBatch(lambda batch_df, _: batch_df.write \
                    .jdbc(url=postgres_options["url"], table=postgres_options["dbtable"], mode="append", properties=postgres_options)) \
        .outputMode("append") \
        .start()

    query.awaitTermination()


#****************************************************************************************************
#*                                              Kafka                                               *
#****************************************************************************************************

def write_to_kafka_in_chunks(df, kafka_bootstrap_servers, kafka_topic, chunk_size):
    try:
        # Add a row number column to partition the data
        window_spec = Window.orderBy("id")  # Replace "id" with a suitable column for ordering if necessary
        df_with_row_num = df.withColumn("row_number", row_number().over(window_spec))

        # Calculate total rows
        total_rows = df_with_row_num.count()
        num_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size != 0 else 0)

        for i in range(num_chunks):
            # Filter rows for the current chunk
            start_row = i * chunk_size + 1
            end_row = (i + 1) * chunk_size
            chunk = df_with_row_num.filter((col("row_number") >= start_row) & (col("row_number") <= end_row)).drop("row_number")

            # Serialize data to JSON and write to Kafka
            (chunk.select(to_json(struct([chunk[col] for col in chunk.columns])).alias("value"))
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("topic", kafka_topic)
                .save())
            logger.info(f"Chunk {i + 1}/{num_chunks} successfully streamed to Kafka topic: {kafka_topic}")

    except Exception as e:
        logger.error("Failed to write data to Kafka in chunks.", exc_info=True)
        raise e
    

def spark_consumer_to_df(spark: SparkSession,topic:str,schema):

    stream_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", topic) \
    .option('startingOffsets', 'latest') \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(F.col('value').cast(T.StringType()))


    parsed_df = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), schema)) \
    .select(F.col('parsed_json.*'))


    return parsed_df 

def procuder_minio_to_kafka(spark:SparkSession,topic,schema):

    df_stream = spark.readStream \
        .format("parquet") \
        .schema(schema)\
        .option("path", f"s3a://{topic}/") \
        .load()

    json_df = df_stream.select(F.to_json(F.struct([col for col in df_stream.columns])).alias("value"))

    query = json_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "course-kafka:9092") \
        .option("topic", topic) \
        .outputMode("update") \
        .option("checkpointLocation", f"s3a://spark/{topic}/checkpoints/") \
        .start()

    query.awaitTermination()






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
    
    