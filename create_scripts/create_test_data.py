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

rds_host = os.getenv("RDS_HOST")
rds_user = os.getenv("RDS_USER")
rds_password = os.getenv("RDS_PASSWORD")
rds_database = os.getenv("RDS_DATABASE")

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

def save_to_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file)

def convert_to_utc(latest_datetime):
    cest = pytz.timezone('Europe/Berlin')
    if latest_datetime.tzinfo is None:
        latest_datetime = cest.localize(latest_datetime)
    return latest_datetime.astimezone(pytz.utc)

def create_test_data():
    smartmate_data = load_json()

    connection = create_connection(host=rds_host, user=rds_user, password=rds_password, database=rds_database)
    try:
        for table in smartmate_data['galera']['tables']:

                query = text(f"SELECT id, created_at, updated_at FROM {table['name']} ORDER BY id DESC LIMIT 10")
                result = connection.execute(query)
                rows = result.fetchall()
                id = rows[9][0]
                created_at = rows[9][1]
                updated_at = rows[9][2]
                table['latest_id'] = id
                table['latest_created_at'] = convert_to_utc(created_at).strftime('%Y-%m-%d %H:%M:%S') if created_at is not None else None
                table['latest_updated_at'] = convert_to_utc(updated_at).strftime('%Y-%m-%d %H:%M:%S') if updated_at is not None else table['latest_created_at'] if created_at else None
    finally:
        connection.close()

    save_to_json(smartmate_data, 'smartmate-schematest.json')

create_test_data()