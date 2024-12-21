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
from minio.commonconfig import REPLACE, CopySource



# Third-Party Libraries
import pandas as pd
import requests
from minio import Minio


# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import connect_to_postgres_data
from Connections.connection import init_minio_client


minio_client = init_minio_client()
bucket_name = 'prices'
prefix = ''
objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)


for i,obj in enumerate(objects):
    file = obj.object_name.split('/')[1]
    folder = file.split('-')[2][:8]
    source_object = CopySource(bucket_name,obj.object_name)
    minio_client.copy_object(bucket_name,f'{file}',source_object)

    print(i,folder,file)
 








