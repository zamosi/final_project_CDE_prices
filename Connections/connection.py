import logging

from psycopg2 import connect
from configparser import ConfigParser
from sqlalchemy import create_engine


# Read database configuration from the config file
config = ConfigParser()
config.read("config/config.conf")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def connect_to_postgres_data():
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
