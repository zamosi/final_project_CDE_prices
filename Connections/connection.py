import logging

from psycopg2 import connect
from configparser import ConfigParser
from sqlalchemy import create_engine

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_json, struct


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


#****************************************************************************************************
#*                                              Kafka                                               *
#****************************************************************************************************

def write_to_kafka(df, kafka_bootstrap_servers, kafka_topic):
    try:
        # Serialize data to JSON and write to Kafka
        (df.select(to_json(struct([df[col] for col in df.columns])).alias("value"))
           .write
           .format("kafka")
           .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
           .option("topic", kafka_topic)
           .save())
        logger.info(f"Data successfully streamed to Kafka topic: {kafka_topic}")
    except Exception as e:
        logger.error("Failed to write data to Kafka.", exc_info=True)
        raise e