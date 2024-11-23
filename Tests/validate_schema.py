# Standard Library Imports
import sys
import os
import logging

# Third-Party Libraries
from sqlalchemy import inspect, engine

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import connect_to_postgres_data

# Set up Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Function to check if schema exists
def check_schema_exists(engine: engine, schema_name: str):
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    
    if schema_name not in schemas:
        raise ValueError(f"Schema '{schema_name}' does not exist.")
    else:
        None


# Function to check if tables exist
def check_tables_exist(engine: engine, schema_name: str, list_of_tables: list):
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
    
    # Connect to the database
    conn, engine = connect_to_postgres_data()    
    
    try:
        schema_name = 'raw_data'
        list_of_tables = ["snifim", "prices"]
        
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
