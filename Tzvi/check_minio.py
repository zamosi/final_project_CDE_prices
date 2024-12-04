# Standard Library Imports
import sys
import os
import logging
import time
import xml.etree.ElementTree as ET
import gzip
from io import BytesIO
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

def table_to_df(table_name:str,engine) ->pd.DataFrame:
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    return df

# conn, engine = connect_to_postgres_data()
# df = table_to_df(table_name='raw_data.reshatot',engine=engine)





# df.to_parquet(
#     's3://check/aaa/reshtot2.parquet',
#     index=False, 
#     storage_options={
#         'key': 'minioadmin',
#         'secret': 'minioadmin',
#         'client_kwargs': {'endpoint_url': 'http://minio:9000'}
#     }
# )
from minio import Minio


minio_client = Minio(
    "minio:9000",  
    access_key="minioadmin", 
    secret_key="minioadmin", 
    secure=False
)

bucket_name = "prices"
objects = minio_client.list_objects(bucket_name, recursive=True)
for i,obj in enumerate(objects):
    print(i,obj.object_name)


