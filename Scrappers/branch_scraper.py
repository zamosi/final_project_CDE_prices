# Standard Library Imports
import sys
import os
import logging
import time
import xml.etree.ElementTree as ET
from configparser import ConfigParser

# Third-Party Libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import create_engine

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import connect_to_postgres_data

# Constants
BASE_URL = 'https://url.publishedprices.co.il/file/d/'
LOGIN_URL = 'https://url.publishedprices.co.il/login'
USERNAME = 'freshmarket'
WAIT_TIME = 10

# Set up Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_driver() -> webdriver.Chrome:
    """Initialize Chrome WebDriver with headless mode."""
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


def login_to_site(driver: webdriver.Chrome) -> None:
    """Log into the website using the provided username."""
    try:
        driver.get(LOGIN_URL)
        driver.find_element(By.NAME, 'username').send_keys(USERNAME)
        driver.find_element(By.ID, 'login-button').click()
        time.sleep(WAIT_TIME)
        logger.info("Login successful.")
    except Exception as e:
        logger.error(f"Error during login: {e}")
        raise


def fetch_page_soup(driver: webdriver.Chrome) -> BeautifulSoup:
    """Retrieve and parse the current page source."""
    try:
        return BeautifulSoup(driver.page_source, "html.parser")
    except Exception as e:
        logger.error(f"Error fetching page source: {e}")
        raise


def get_session_with_cookies(driver: webdriver.Chrome) -> requests.Session:
    """Create a requests session with cookies from the WebDriver."""
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
    """Find links to XML files on the page."""
    try:
        xml_files = [a.get("title") for a in soup.find_all("a", class_="f")
                     if a.get("title", "").startswith("Stores") and a.get("title", "").endswith(".xml")]
        logger.info(f"Found {len(xml_files)} XML file(s).")
        return xml_files
    except Exception as e:
        logger.error(f"Error finding XML links: {e}")
        raise


def download_and_parse_xml(session: requests.Session, file_url: str) -> pd.DataFrame:
    """Download and parse XML content from the provided URL."""
    try:
        response = session.get(file_url)
        response.encoding = 'utf-16'
        root = ET.fromstring(response.text)
        logger.info(f"Downloaded and parsed XML file from {file_url}")
        return parse_xml_to_dataframe(root)
    except (requests.RequestException, ET.ParseError) as e:
        logger.error(f"Error processing XML file from {file_url}: {e}")
        raise


def parse_xml_to_dataframe(root: ET.Element) -> pd.DataFrame:
    """Convert XML content to a DataFrame with RTL adjustments and uppercase columns."""
    try:
        data = [{child.tag: child.text for child in store}
                for store in root.findall('.//Stores/Store')]
        df = pd.DataFrame(data)
        df.columns = [col.lower() for col in df.columns]

        # Adjust specific columns for RTL by reversing text
        for col in ['ADDRESS', 'STORENAME', 'CITY']:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: x[::-1] if isinstance(x, str) else x)

        logger.info("Converted XML content to DataFrame with RTL adjustments.")
        return df
    except Exception as e:
        logger.error(f"Error converting XML content to DataFrame: {e}")
        raise

def insert_dataframe_to_postgres(engine, df: pd.DataFrame, table_name: str):
    """Inserts a DataFrame into a specified PostgreSQL table."""
    try:
        df.to_sql(table_name, con=engine, schema='raw_data', if_exists='replace', index=False)
        logger.info(f"Data inserted successfully into {table_name}.")
    except Exception as e:
        logger.error(f"Error inserting data into PostgreSQL: {e}")

def main():
    target_table_name = 'snifim'
    
    driver = setup_driver()
    try:
        login_to_site(driver)
        soup = fetch_page_soup(driver)
        xml_files = find_xml_links(soup)

        if not xml_files:
            logger.warning("No XML files found.")
            return

        session = get_session_with_cookies(driver)
        file_url = f"{BASE_URL}{xml_files[0]}"
        logger.info(f"Fetching file: {file_url}")

        df = download_and_parse_xml(session, file_url)
        logger.info(f"DataFrame created with {len(df)} rows.")
    except Exception as e:
        logger.critical(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        driver.quit() if driver else None  
        logger.info("WebDriver session closed.")

    try:
        conn, engine = connect_to_postgres_data()
        if conn:
            insert_dataframe_to_postgres(engine, df, target_table_name)
    except Exception as e:
        logger.critical(f"An error occurred with database operations: {e}")
    finally:
        conn.close() if conn else None    
        engine.dispose() if engine else None
        logger.info('Connection closed.')



if __name__ == "__main__":
    main()
