# Standard Library Imports
import sys
import os
import logging
import re
from configparser import ConfigParser

# Third-Party Libraries
from sqlalchemy import inspect, engine

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import connect_to_postgres_data
import SQL


# Load configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")


# Set up Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_sql_file(file_path:str) -> list:
    """
    This function reads the content of the provided SQL file, searches for
    `CREATE TABLE` statements, and extracts the table names that follow the
    `schema_name.table_name` format.

    Args:
        file_path (str): The path to the SQL file to be parsed.

    Returns:
        list: A list of table names (as strings) found in the SQL file.
              If no table names are found, an empty list is returned.
              
    Example:
        >>> parse_sql_file("schema.sql")
        ['table1', 'table2']
    """
    with open(file_path, 'r') as file:
        sql_content = file.read()
        
    return re.findall(r'CREATE TABLE\s+[a-zA-Z_]+\.(\w+)', sql_content)


# Function to check if schema exists
def check_schema_exists(engine: engine, schema_name: str):
    """
    Checks if the specified schema exists in the database.

    Args:
        engine (engine): Database engine connection.
        schema_name (str): Name of the schema to validate.

    Raises:
        ValueError: If the schema does not exist in the database.
    """    
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    
    if schema_name not in schemas:
        raise ValueError(f"Schema '{schema_name}' does not exist.")
    else:
        None


# Function to check if tables exist
def check_tables_exist(engine: engine, schema_name: str, list_of_tables: list):
    """
    Validates that all specified tables exist in the given schema int the DB.

    Args:
        engine (engine): Database engine connection.
        schema_name (str): Name of the schema to check.
        list_of_tables (list): List of table names to validate.

    Raises:
        ValueError: If any table in the list does not exist in the schema in DB.
    """    
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names(schema=schema_name)
    
    # Enforce lowercase for PostgreSQL compatibility
    list_of_tables = [tab.lower() for tab in list_of_tables]
    
    for table in list_of_tables:
        if table not in existing_tables:
            raise ValueError(f"Table '{table}' does not exist in schema '{schema_name}'.")
        else:
            None


def main():
    
    sql_file_path = config["Core_Settings"]["SQL_SCHEMA_FILE_PATH"]

    # Connect to the database
    conn, engine = connect_to_postgres_data()    
    
    try:
        schema_name = 'raw_data'
        list_of_tables = parse_sql_file(sql_file_path)
        
        # Check schema and tables
        check_schema_exists(engine, schema_name)
        check_tables_exist(engine, schema_name, list_of_tables)   

        logger.info(f"Validation of schema: {schema_name} ended with success, "
                    f"lists for check: {', '.join(list_of_tables)}")
        
    except ValueError as e:
        # Catch errors and stop execution
        logger.critical(e)
        sys.exit(1)
    except Exception as e:
        # General error handling
        logger.critical(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        conn.close()
        engine.dispose()

if __name__ == '__main__':
    main()
