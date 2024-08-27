import pytest
from sqlalchemy import create_engine, text
import os
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

def compare_row_counts(table, connection1, connection2):
    query = text(f"SELECT COUNT(*) FROM {table}")
    
    result1 = connection1.execute(query).scalar()
    result2 = connection2.execute(query).scalar()
    
    return result1 == result2, result1, result2

@pytest.fixture(scope="module")
def db_connections():
    connection1 = create_connection(host=db1_host, user=db1_user, password=db1_password, database=db1_database)
    connection2 = create_connection(host=db2_host, user=db2_user, password=db2_password, database=db2_database)
    
    yield connection1, connection2
    
    connection1.close()
    connection2.close()

@pytest.mark.parametrize("table", [
    'charts', 'clients', 'comments', 'departments', 'devices', 'documents',
    'exception_tasks', 'facilities', 'facility_services', 'gateways', 
    'logs', 'measuring_thresholds', 'programs', 'reports', 'schedules', 
    'sensor_alarms', 'sensors', 'sms_logs', 'staff_time_log', 'staff_users',
    'subtasks', 'subtasks_translations', 'tasks', 'tasks_translations', 
    'users', 'audits', 'oauth_access_tokens', 'password_resets', 
    'model_has_roles', 'templates_entities', 'users_departments', 'users_sensors'
])
def test_row_counts(db_connections, table):
    connection1, connection2 = db_connections
    
    is_equal, count1, count2 = compare_row_counts(table, connection1, connection2)
    
    assert is_equal, f"{table}: FAILED - {db1_host} has {count1} rows, {db1_host} has {count2} rows."


if __name__ == "__main__":
    pytest.main()
