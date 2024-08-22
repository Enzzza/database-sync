# Database Sync

## Overview

This repository contains scripts to facilitate the synchronization between an RDS (Relational Database Service) database and a local MySQL database. The scripts are organized into two main directories: `create_scripts` and `scripts`.

### Directory Structure

```
.
├── create_scripts
│   ├── create_sql.py
│   ├── create_test_data.py
│   ├── test-connection.py
│   ├── test.py
├── scripts
│   ├── cleanup.sh
│   ├── insert_sql.sh
│   ├── move_insert_csv.sh
│   ├── move_special_update_csv.sh
│   ├── move_update_csv.sh
│   ├── special_update_sql.sh
│   ├── update_sql.sh
└── tables
    ├── insert
    │   └── [table-name]
    │       ├── [table-name]_csv.sql
    │       ├── [table-name]_load_csv.sql
    │       └── insert-[table-name].csv
```

## Files and Their Functions

### `create_scripts` Directory

1. **`create_sql.py`**:
   - Generates the necessary SQL scripts for creating tables.
   - Creates a `tables` directory at the top level. Each table directory contains the following:
     - **`[table-name]_csv.sql`**: The SQL query used to extract data from the RDS database into a CSV file.
     - **`[table-name]_load_csv.sql`**: The SQL query used to load the CSV data into the local MySQL database.
     - **`insert-[table-name].csv`**: The CSV file containing the extracted data.

2. **`create_test_data.py`**:
   - Extracts the last 10 records from each table in the RDS database.
   - Saves the data into a test schema for validation purposes.

3. **`test-connection.py`**:
   - A helper script to test the connection to the RDS server.

4. **`test.py`**:
   - A pytest script to check if the data in the RDS and local MySQL databases are synchronized.

### `scripts` Directory

1. **`move_insert_csv.sh`**:
   - Sets the required permissions and file group for all CSV files.
   - Moves the files to MySQL's secure directory for further processing.

2. **`insert_sql.sh`**:
   - Executes the SQL queries found in the `tables` directory to load the data into the local MySQL database.

3. **`move_update_csv.sh`**:
   - Similar to `move_insert_csv.sh`, but for handling CSV files related to updates.

4. **`update_sql.sh`**:
   - Executes SQL queries to update the local MySQL database with data from the CSV files.

5. **`move_special_update_csv.sh`**:
   - Handles permissions and movements for special update CSV files.

6. **`special_update_sql.sh`**:
   - Executes special update SQL queries on the local MySQL database.

7. **`cleanup.sh`**:
   - A script to clean up temporary files and directories after processing.

## Usage

1. **Generate SQL and CSV files**:
   - Run `create_sql.py` to generate the necessary SQL scripts and extract data into CSV files.

2. **Move and Load CSV files**:
   - Use `move_insert_csv.sh` to prepare and move the CSV files to the MySQL server.
   - Run `insert_sql.sh` to load the data from CSV files into the local MySQL database.

3. **Test the Synchronization**:
   - Run `test.py` to ensure the RDS and local MySQL databases are synchronized.

4. **Update Data**:
   - Use `move_update_csv.sh` and `update_sql.sh` to update the local database with new data from the RDS.

5. **Cleanup**:
   - Run `cleanup.sh` to remove any temporary files generated during the process.

## Notes

- Ensure that the necessary permissions and group settings are configured correctly when moving files between directories.
- Modify the script paths and database credentials according to your environment.

This repository is designed to automate the process of syncing data between an RDS database and a local MySQL database, ensuring data consistency and integrity across environments.