# Standard Library Imports
import sys
import os
import logging
import time
import xml.etree.ElementTree as ET

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


def login_to_site(driver: webdriver.Chrome) -> None:
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

    Args:
        driver (webdriver.Chrome): The WebDriver instance used to retrieve cookies.

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

    Args:
        soup (BeautifulSoup): The BeautifulSoup object containing parsed HTML content.

    Returns:
        list: A list of titles (links) to XML files found on the page.
    """
    try:
        xml_files = [a.get("title") for a in soup.find_all("a", class_="f")
                     if a.get("title", "").startswith("Stores") and a.get("title", "").endswith(".xml")]
        logger.info(f"Found {len(xml_files)} XML file(s).")
        return xml_files
    except Exception as e:
        logger.error(f"Error finding XML links: {e}")
        raise


def download_and_parse_xml(session: requests.Session, file_url: str) -> pd.DataFrame:
    """
    Uses a `requests.Session` to download the XML file, decodes it with UTF-16 
    encoding, parses it into an ElementTree object, and converts it into a 
    DataFrame using `parse_xml_to_dataframe`.

    Args:
        session (requests.Session): The session used to download the XML file.
        file_url (str): The URL of the XML file to be downloaded.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the parsed XML data.
    """
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
    """
    Uses the SQLAlchemy engine to insert data into a PostgreSQL database. The data
    is inserted into the specified table under the 'raw_data' schema. Existing
    data in the table is replaced.

    Args:
        engine: SQLAlchemy engine connected to the PostgreSQL database.
        df (pd.DataFrame): The DataFrame to be inserted into the database.
        table_name (str): The name of the PostgreSQL table to insert the data into.
    """
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

        # Extract XML file links from the webpage
        xml_files = find_xml_links(soup)

        if not xml_files:
            logger.warning("No XML files found.")
            return

        # Create a session with cookies to use for file download
        session = get_session_with_cookies(driver)
        file_url = f"{BASE_URL}{xml_files[0]}"
        logger.info(f"Fetching file: {file_url}")

        # Download XML file and parse it into a DataFrame
        df = download_and_parse_xml(session, file_url)
        logger.info(f"DataFrame created with {len(df)} rows.")
    except Exception as e:
        logger.critical(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        # Ensure the WebDriver is properly closed
        driver.quit() if driver else None
        logger.info("WebDriver session closed.")

    try:
        # Connect to PostgreSQL and insert the DataFrame
        conn, engine = connect_to_postgres_data()
        if conn:
            insert_dataframe_to_postgres(engine, df, target_table_name)
    except Exception as e:
        logger.critical(f"An error occurred with database operations: {e}")
    finally:
        # Clean up database resources
        conn.close() if conn else None    
        engine.dispose() if engine else None
        logger.info('Connection closed.')



if __name__ == "__main__":
    main()
