from sqlalchemy import create_engine, text
import pytz
import pandas as pd
import json
import os
from dotenv import load_dotenv
import time
import csv
from multiprocessing import Process


start_time = time.time()
load_dotenv('../.env')
print("Loading env variables...")

rds_host = os.getenv("RDS_HOST")
rds_user = os.getenv("RDS_USER")
rds_password = os.getenv("RDS_PASSWORD")
rds_database = os.getenv("RDS_DATABASE")

galera_host = os.getenv("GALERA_HOST")
galera_user = os.getenv("GALERA_USER")
galera_password = os.getenv("GALERA_PASSWORD")
galera_database = os.getenv("GALERA_DATABASE")

schema_file = os.getenv("SCHEMA_FILE")
dest_dir = os.getenv("DEST_DIR")
source_dir = os.getenv("SOURCE_DIR")


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


def get_latest(smartmate_data):
    print("Getting latest data...")
    connection = create_connection(host=galera_host, user=galera_user, password=galera_password, database=galera_database)
    try:
        for table in smartmate_data['galera']['tables']:
            query = text(f"SELECT id, created_at, updated_at FROM {table['name']} ORDER BY id DESC LIMIT 1")
            print(f"Executing query... {query}")
            result = connection.execute(query).fetchone()
            table['latest_id'] = result[0]
            table['latest_created_at'] = convert_to_utc(result[1]).strftime('%Y-%m-%d %H:%M:%S') if result[1] is not None else None
            table['latest_updated_at'] = convert_to_utc(result[2]).strftime('%Y-%m-%d %H:%M:%S') if result[2] is not None else None
            if table['latest_created_at'] and table['latest_updated_at'] == None:
                table['latest_updated_at'] = table['latest_created_at']
                
    finally:
        connection.close()


def generate_csv_query(table_name, fields, csv_name, field_terminator=',', enclosure='"', line_terminator='\\n', ignore_lines=1, duplicate_handling='IGNORE', disable_constraints=False , truncate_table=False):
    csv_path = f'{dest_dir}/{csv_name}'
    
    timestamp_columns = ['created_at', 'updated_at', 'deleted_at']
    field_list = []
    set_clauses = []

    for field in fields:
        field_with_backticks = f"`{field}`"
        placeholder = f"@{field}"

        # Handle timestamp fields
        if field in timestamp_columns:
            set_clauses.append(
                f"{field_with_backticks} = IF({placeholder} = '', NULL, STR_TO_DATE({placeholder}, '%Y-%m-%d %H:%i:%s'))"
            )
        else:
            # Handle other fields
            set_clauses.append(
                f"{field_with_backticks} = IF({placeholder} = '', NULL, {placeholder})"
            )
        
        field_list.append(placeholder)

    field_list_str = ', '.join(field_list)
    set_clauses_str = ', '.join(set_clauses) if set_clauses else ''

    query_template = """
{set_constraints_begining}
{truncate_table}
LOAD DATA INFILE '{csv_path}'
{duplicate_handling}
INTO TABLE `{table_name}`
FIELDS TERMINATED BY '{field_terminator}'
ENCLOSED BY '{enclosure}'
ESCAPED BY ''
LINES TERMINATED BY '{line_terminator}'
IGNORE {ignore_lines} LINES
({field_list_str})
{set_clauses_str};
{set_constraints_end}
    """
    
    query = query_template.format(
        csv_path=csv_path,
        table_name=table_name,
        field_terminator=field_terminator,
        enclosure=enclosure,
        line_terminator=line_terminator,
        ignore_lines=ignore_lines,
        field_list_str=field_list_str,
        set_clauses_str=f"SET {set_clauses_str}" if set_clauses_str else '',
        duplicate_handling=duplicate_handling,
        set_constraints_begining="SET foreign_key_checks = 0;" if disable_constraints else '',
        set_constraints_end="SET foreign_key_checks = 1;" if disable_constraints else '',
        truncate_table=f"TRUNCATE TABLE {table_name};" if truncate_table else '',

    )
    
    return query


def create_insert_csv_files(smartmate_data):
    print("Creating insert csv files...")
    connection = create_connection(host=rds_host, user=rds_user, password=rds_password, database=rds_database)
    try:
        for table in smartmate_data['galera']['tables']:
            file_name = f"{table['rank']}-{table['name']}"
            directory = os.path.join(source_dir, 'insert', file_name)
            os.makedirs(directory, exist_ok=True)           
            if 'latest_id' in table and table['latest_id'] is not None:
                query = text(f"SELECT * FROM {table['name']} WHERE id > {table['latest_id']}")
            elif 'latest_created_at' in table and table['latest_created_at'] is not None:
                latest_created_at = table['latest_created_at']

                query = text(f"SELECT * FROM {table['name']} WHERE created_at > '{latest_created_at}'")
            elif 'latest_updated_at' in table and table['latest_updated_at'] is not None:
                latest_updated_at = table['latest_updated_at']
                query = text(f"SELECT * FROM {table['name']} WHERE updated_at > '{latest_updated_at}'")
            else:
                continue
            
            # Save csv query to file
            with open(f"{directory}/{file_name}_csv.sql", 'w') as file:
                file.write(str(query))        
            print(f"Executing query... {query}")  
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            csv_name = f"insert-{file_name}.csv"
            # Save csv to file
            df.to_csv(f"{directory}/{csv_name}", index=False, quoting=csv.QUOTE_ALL)
            
            csv_query = generate_csv_query(table['name'], result.keys(), csv_name)          
            # Save load csv query to file
            with open(f"{directory}/{file_name}_load_csv.sql", 'w') as file:
                file.write(csv_query)

    finally:
        connection.close()


def create_special_insert_csv_file(table):
    connection = create_connection(host=rds_host, user=rds_user, password=rds_password, database=rds_database)
    executed_queries = []
    file_name = f"{table['rank']}-{table['name']}"
    csv_name = f"special-insert-{file_name}.csv"
    directory = os.path.join(source_dir, 'special_insert', file_name)
    os.makedirs(directory, exist_ok=True)         
    chunk_size = 10000
    offset = 0
    try:
        while True:
            query = text(f"SELECT * FROM {table['name']} LIMIT {chunk_size} OFFSET {offset}")
            executed_queries.append(query)
            print(f"Executing query... {query}")
            result = connection.execute(query)
            if not result.rowcount:
                break
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            # Save csv to file
            df.to_csv(f"{directory}/{csv_name}", mode='a', index=False, quoting=csv.QUOTE_ALL)
            offset += chunk_size
        # Save csv query to file
        with open(f"{directory}/{file_name}_csv.sql", 'w') as file:
            file.write('\n'.join([str(query) for query in executed_queries]))
        csv_query = generate_csv_query(table['name'], result.keys(), csv_name, duplicate_handling='IGNORE', disable_constraints=True, truncate_table=True)
        # Save load csv query to file
        with open(f"{directory}/{file_name}_load_csv.sql", 'w') as file:
            file.write(csv_query)
    finally:
        connection.close()



def create_special_insert_csv_files(smartmate_data):
    print("Creating special update csv files...")
    proccesses = []
    
    for table in smartmate_data['galera']['special_tables']:
        p = Process(target=create_special_insert_csv_file, args=(table,))
        proccesses.append(p)
        p.start()
    for p in proccesses:
        p.join()


smartmate_data = load_json()
get_latest(smartmate_data)
create_insert_csv_files(smartmate_data)
create_special_insert_csv_files(smartmate_data)


end_time = time.time()
execution_time = end_time - start_time
print("Done")
print(f"Execution time: {execution_time} seconds")
