from sqlalchemy import create_engine, text
import pytz
import pandas as pd
import json
import os
from dotenv import load_dotenv
import time

start_time = time.time()
load_dotenv()
print("Loading env variables...")

galera_host = os.getenv("GALERA_HOST")
galera_user = os.getenv("GALERA_USER")
galera_password = os.getenv("GALERA_PASSWORD")
galera_database = os.getenv("GALERA_DATABASE")

schema_file = os.getenv("TEST_SCHEMA_FILE")

def create_connection(host, user, password, database):
    print(f"Creating connection to {host}...")
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        connection = engine.connect()
        return connection
    except Exception as e:
        print(f"Failed to connect to {host} with error: {e}")
        return None

def load_json():
    print("Loading smartmate schema...")
    with open(schema_file, 'r') as file:
        data = file.read()
        return json.loads(data)

def create_test_connection():
    smartmate_data = load_json()

    connection = create_connection(host=galera_host, user=galera_user, password=galera_password, database=galera_database)
    try:
        for table in smartmate_data['galera']['tables']:

                query = text(f"SELECT id, created_at, updated_at FROM {table['name']} ORDER BY id DESC LIMIT 1")
                result = connection.execute(query)
                rows = result.fetchall()
                print(rows)
    finally:
        connection.close()


create_test_connection()