import os
import pytest
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
print("Loading env variables...")

db1_host = os.getenv("GALERA_HOST")
db1_user = os.getenv("GALERA_USER")
db1_password = os.getenv("GALERA_PASSWORD")
db1_database = os.getenv("GALERA_DATABASE")

db2_host = os.getenv("RDS_HOST")
db2_user = os.getenv("RDS_USER")
db2_password = os.getenv("RDS_PASSWORD")
db2_database = os.getenv("RDS_DATABASE")

def create_connection(host, user, password, database):
    print(f"Creating connection to {host}...")
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        connection = engine.connect()
        return connection
    except Exception as e:
        print(f"Error creating connection: {e}")
        return None

def fetch_rows(connection, table, record_id):
    if connection is None:
        return None
    query = text(f"SELECT * FROM {table} WHERE id = :record_id")
    result = connection.execute(query, {'record_id': record_id}).fetchone()
    return dict(result._mapping) if result else None

def get_ids(connection, table):
    if connection is None:
        return []
    query = text(f"SELECT id FROM {table}")
    result = connection.execute(query).fetchall()
    return [row[0] for row in result]

@pytest.fixture(scope="module")
def db_connections():
    connection1 = create_connection(host=db1_host, user=db1_user, password=db1_password, database=db1_database)
    connection2 = create_connection(host=db2_host, user=db2_user, password=db2_password, database=db2_database)
    
    if connection1 is None or connection2 is None:
        pytest.skip("Skipping tests due to failed database connection.")
    
    yield connection1, connection2
    
    if connection1 is not None:
        connection1.close()
    if connection2 is not None:
        connection2.close()

def normalize_data(record):
    """
    Normalize data by converting empty strings to None in the record.
    """
    if record:
        for key in record:
            if record[key] == "":
                record[key] = None
    return record

@pytest.mark.parametrize("table", [
    'users'
])
def test_field_comparisons(db_connections, table):
    connection1, connection2 = db_connections
    
    ids = get_ids(connection1, table)
    
    for record_id in ids:
        record1 = fetch_rows(connection1, table, record_id)
        record2 = fetch_rows(connection2, table, record_id)
        
        assert record2 is not None, f"Record with ID {record_id} not found in the second database for table '{table}'."
        
        record1 = normalize_data(record1)
        record2 = normalize_data(record2)
        
        assert record1 == record2, (
            f"Mismatch in table '{table}', ID {record_id}:\n"
            f"{db1_host}='{record1}', {db2_host}='{record2}'"
        )

if __name__ == "__main__":
    pytest.main()
