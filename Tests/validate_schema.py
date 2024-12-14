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

def parse_sql_file(file_path: str) -> list:
    """
    This function reads the content of the provided SQL file, searches
    `CREATE TABLE` statements, and extracts the table names that follow the
    `schema_name.table_name` format.

    Returns:
        A list of table names (as strings) found in the SQL file.
        If no table names are found, an empty list is returned.

    """
    with open(file_path, 'r') as file:
        sql_content = file.read()
        
    return re.findall(r'CREATE TABLE\s+[a-zA-Z_]+\.(\w+)', sql_content)

# Function to check if schema exists
def check_schema_exists(engine: engine, schema_name: str):
    """
    Checks if the specified schema exists in the database.

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
def check_tables_exist(engine: engine, schema_names: list, list_of_tables: list):
    """
    Validates that all specified tables exist in at least one of the given schemas in the DB.

    Raises:
        ValueError: If any table in the list does not exist in any of the schemas in DB.
    """    
    inspector = inspect(engine)
    existing_tables_by_schema = {
        schema: inspector.get_table_names(schema=schema) for schema in schema_names
    }

    # Enforce lowercase for PostgreSQL compatibility
    list_of_tables = [tab.lower() for tab in list_of_tables]

    for table in list_of_tables:
        if not any(table in existing_tables_by_schema[schema] for schema in schema_names):
            raise ValueError(f"Table '{table}' does not exist in any of the schemas: {', '.join(schema_names)}.")
        else:
            None

def validate_schema_and_tables(engine: engine, schema_names: list, sql_file_path: str):
    """
    Validates the existence of schemas and their associated tables.

    Raises:
        ValueError: If schemas or any table does not exist.
    """
    list_of_tables = parse_sql_file(sql_file_path)

    # Check schemas
    for schema_name in schema_names:
        check_schema_exists(engine, schema_name)

    # Check tables across schemas
    check_tables_exist(engine, schema_names, list_of_tables)

    logger.info(f"Validation of schemas: {', '.join(schema_names)} ended with success, "
                f"lists for check: {', '.join(list_of_tables)}")

def main():
    # Paths to sql files with required schema definitions
    sql_file_path = config["Core_Settings"]["SQL_SCHEMA_FILE_PATH"]
    
    # Connect to the database
    conn, engine = connect_to_postgres_data()    
    
    try:
        # Validate raw_data and dwh schemas
        validate_schema_and_tables(engine, ['raw_data', 'dwh'], sql_file_path)

    except ValueError as e:
        logger.critical(e)
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        conn.close()
        engine.dispose()

if __name__ == '__main__':
    main()
