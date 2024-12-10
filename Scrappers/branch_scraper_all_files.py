# Standard Library Imports
import sys
import os
import logging
import time
import xml.etree.ElementTree as ET
import gzip
from io import BytesIO
import io
from datetime import datetime
import re


# Third-Party Libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import connect_to_postgres_data
from Connections.connection import init_minio_client


# Constants
BASE_URL = 'https://url.publishedprices.co.il/file/d/'
LOGIN_URL = 'https://url.publishedprices.co.il/login'
# USERNAME = 'freshmarket'
WAIT_TIME = 20

# Set up Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def table_to_df(table_name:str,engine) ->pd.DataFrame:
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        logger.info(f"Table {table_name} loaded successfully.")
        return df
    except Exception as e:
        logger.error(f"An error occurred: {e}")

def setup_driver() -> webdriver.Chrome:
    """    
    This function sets up a Chrome WebDriver with headless mode and additional 
    arguments for improved compatibility and performance in non-GUI environments.

    Returns:
        webdriver.Chrome: A Chrome WebDriver instance.
    """
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        logger.error(f"Error setting up WebDriver: {e}")
        raise

def login_to_site(driver: webdriver.Chrome,USERNAME:str,PASSWORD:str) -> None:
    """
    Navigates to the login page, enters the username, and submits the login form.
    Includes error handling and logging to ensure any issues during the login
    process are reported.

    Args:
        driver (webdriver.Chrome): The WebDriver instance used to interact with the website.
    """
    try:
        driver.get(LOGIN_URL)
        driver.find_element(By.NAME, 'username').send_keys(USERNAME)
        driver.find_element(By.NAME, 'password').send_keys(PASSWORD)
        driver.find_element(By.ID, 'login-button').click()
        time.sleep(WAIT_TIME)
        logger.info("Login successful.")
    except Exception as e:
        logger.error(f"Error during login: {e}")
        raise

def fetch_page_soup(driver: webdriver.Chrome) -> BeautifulSoup:
    """
    This function fetches the HTML content of the currently loaded page in the 
    WebDriver and parses it into a BeautifulSoup object for further processing.

    Args:
        driver (webdriver.Chrome): The WebDriver instance used to interact with the website.

    Returns:
        BeautifulSoup: Parsed HTML content of the current page.
    """
    try:
        return BeautifulSoup(driver.page_source, "html.parser")
    except Exception as e:
        logger.error(f"Error fetching page source: {e}")
        raise

def get_session_with_cookies(driver: webdriver.Chrome) -> requests.Session:
    """
    Extracts cookies from the current WebDriver instance and transfers them
    to a `requests.Session` for use in making HTTP requests outside of the WebDriver.

    Returns:
        requests.Session: A `requests` session with the cookies from the WebDriver.
    """
    try:
        session = requests.Session()
        for cookie in driver.get_cookies():
            session.cookies.set(cookie['name'], cookie['value'])
        logger.info("Session created with cookies.")
        return session
    except Exception as e:
        logger.error(f"Error setting up session with cookies: {e}")
        raise

def find_xml_links(soup: BeautifulSoup) -> list:
    """
    Searches for tags with the specified class and filters for titles 
    that start with "Store" and end with ".xml".

    Returns:
        list: A list of titles (links) to XML files found on the page.
    """
    try:
        xml_files = [a.get("title") for a in soup.find_all("a", class_="f")
                     if a.get("title", "").startswith(("Price","Store"))]
        logger.info(f"Found {len(xml_files)} XML file(s).")
        return xml_files
    except Exception as e:
        logger.error(f"Error finding XML links: {e}")
        raise

def download_and_parse_xml(session: requests.Session, file_url: str,file_name:str) -> pd.DataFrame:
    """
    Uses a `requests.Session` to download and extract the gz file to xml file, decodes it with UTF-8 
    encoding, parses it into an ElementTree object, and converts it into a 
    DataFrame using `parse_xml_to_dataframe`.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the parsed XML data.
    """
    try:
        if file_name.startswith("Price"):
        
            response = session.get(file_url)
            with gzip.open(BytesIO(response.content), 'rb') as gz_file:
                extracted_content = gz_file.read()
            xml_content = extracted_content.decode('utf-8')
            root = ET.fromstring(xml_content)
            path = './/Items/Item'
        else:
            response = session.get(file_url)
            response.encoding = 'utf-16'
            root = ET.fromstring(response.text)
            path = './/Stores/Store'
        logger.info(f"Downloaded and parsed XML file from {file_url}")
        df = parse_xml_to_dataframe(root,path)
        return add_columns_to_df(df,file_name)
    except (requests.RequestException, ET.ParseError) as e:
        logger.error(f"Error processing XML file from {file_url}: {e}")
        raise

def parse_xml_to_dataframe(root: ET.Element,path:str) -> pd.DataFrame:
    try:
        data = [{child.tag: child.text for child in item}
                for item in root.findall(path)]
        df = pd.DataFrame(data) 
        df.columns = [col.lower() for col in df.columns]

        logger.info("Converted XML content to DataFrame with RTL adjustments.")
        return df
    except Exception as e:
        logger.error(f"Error converting XML content to DataFrame: {e}")
        raise

def add_columns_to_df(df:pd.DataFrame,file_name: str)-> pd.DataFrame:
    date_string = file_name.split('-')[2].replace('.gz','') if file_name.startswith("Price") else file_name.split('-')[1].replace('.xml','')
    df['file_name'] =file_name
    df['num_reshet'] =file_name.split('-')[0].replace('PriceFull','').replace('Price','').replace('Stores','')
    if file_name.startswith("Price"):
        df['num_snif'] =file_name.split('-')[1] 
    df['file_date'] = datetime.strptime(date_string, "%Y%m%d%H%M")
    df['run_time'] = datetime.now()
    return df

def insert_dataframe_to_postgres(engine, df: pd.DataFrame, table_name: str):
    """
    Uses the SQLAlchemy engine to insert data into a PostgreSQL database. The data
    is inserted into the specified table under the 'raw_data' schema. Existing
    data in the table is replaced.

    """
    try:
        df.to_sql(table_name, con=engine, schema='raw_data', if_exists='append', index=False)
        logger.info(f"Data inserted successfully into {table_name}.")
    except Exception as e:
        logger.error(f"Error inserting data into PostgreSQL: {e}")

def check_if_file_exists(minio_client,bucket_name,file_name):
    """
    Check in file exist in a MinIO bucket .
    """
    try:
        minio_client.stat_object(bucket_name, file_name)
        return True  
    except Exception as e:
        return False    

def upload_to_minio(minio_client, bucket_name,file_name,df:pd.DataFrame):
    """
    Uploads a file to a specified folder in a MinIO bucket .
    """
    try:
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        # Upload the file
        minio_client.put_object(bucket_name,file_name,parquet_buffer,len(parquet_buffer.getvalue()))
        logger.info(f"File '{file_name}' uploaded to '{bucket_name}'.")
    except Exception as e:
        logger.error(f"Failed to upload file to MinIO: {e}")



def main():
    error_file = []
    try:
        # Connect to PostgreSQL and insert the DataFrame
        conn, engine = connect_to_postgres_data()

        df = table_to_df(table_name='raw_data.reshatot',engine=engine)
        df.fillna("", inplace=True)
    except Exception as e:
        logger.critical(f"An error occurred with database operations: {e}")

    try:
        # Init Minio client
        minio_client = init_minio_client()
        # bucket_name = 'prices'

        current_date = datetime.now().strftime('%Y%m%d')
        prices_folder_name = f'prices_{current_date}'
        snifim_folder_name = 'snifim'


        for row in df.itertuples():
            driver = setup_driver()
            USERNAME = row.user_name
            PASSWORD = row.password
            logger.info(f"start user name {USERNAME}")

            login_to_site(driver,USERNAME,PASSWORD)
            soup = fetch_page_soup(driver)
            session = get_session_with_cookies(driver)
            # Extract XML file links from the webpage
            xml_files = find_xml_links(soup)

            if not xml_files:
                logger.warning("No XML files found.")
                continue
            
            for i,file in enumerate(xml_files):
                # Create a session with cookies to use for file download
                file_url = f"{BASE_URL}{file}"
                logger.info(f"Fetching file: {file} file{i}")

                target_table_name = 'prices' if file.startswith("Price") else 'snifim'
                
                #Download XML file and parse it into a DataFrame
                try:
                    file_n = file.replace('.gz','.parquet') if file.startswith("Price") else file.replace('.xml','.parquet')

                    if check_if_file_exists(minio_client, target_table_name,file_n):
                        logger.info(f"The file '{file_n}' exists in bucket '{target_table_name}'.")
                    else:
                        df = download_and_parse_xml(session, file_url,file)
                        logger.info(f"DataFrame created with {len(df)} rows.")
                        upload_to_minio(minio_client, target_table_name,file_n,df)

                except Exception as e:
                    logger.error(f"An error occurred when file download - {file}:{e}")
                    error_file.append(file)


            driver.quit() if driver else None
            logger.info(f"WebDriver session {USERNAME} closed.")
            
            
    except Exception as e:
        logger.critical(f"An error occurred: {e}")
        sys.exit(1)

    # Close connections in the end of the process
    print(error_file)


if __name__ == "__main__":
    main()
