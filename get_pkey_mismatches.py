import pandas as pd
import pyodbc
import random
from datetime import datetime
import os
from dotenv import load_dotenv
import random
import warnings

# Suppress the warning
warnings.filterwarnings("ignore", category=UserWarning)

load_dotenv()
source_server = os.getenv("db_server")
source_db_user = os.getenv("azure_db_user")
source_db_password = os.getenv("azure_db_password")
source_database = os.getenv("database")

dest_server = os.getenv("dest_db_server")
dest_db_user = os.getenv("dest_db_user")
dest_db_password = os.getenv("dest_db_password")
dest_database = os.getenv("dest_database")


def connect_to_db(server, db_user, db_password, database):
    conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={db_user};PWD={db_password}"
    conn = pyodbc.connect(conn_string)
    return conn


def check_non_matching_ids(source_conn, dest_conn, tables):
    results = {}
    for table in tables:
        try:
            # Check if table has an identity column in the source database
            identity_check_query = f"""SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                                       WHERE TABLE_NAME = '{table}' AND COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1"""
            identity_df_source = pd.read_sql(identity_check_query, source_conn)
            identity_df_dest = pd.read_sql(identity_check_query, dest_conn)

            # If either database's table doesn't have an identity column, skip it
            if identity_df_source.empty or identity_df_dest.empty:
                print(f"Skipping table {table} as it doesn't have an identity column.")
                continue

            identity_col = identity_df_source.iloc[0]['COLUMN_NAME']

            # Get max(id) from the source table
            source_query = f"SELECT MAX({identity_col}) AS max_id FROM {table}"
            source_max_id = pd.read_sql(source_query, source_conn).iloc[0]['max_id']

            # Get max(id) from the destination table
            dest_query = f"SELECT MAX({identity_col}) AS max_id FROM {table}"
            dest_max_id = pd.read_sql(dest_query, dest_conn).iloc[0]['max_id']

            # Compare max(id) values and store the result
            if source_max_id != dest_max_id:
                results[table] = {'source_max_id': source_max_id, 'dest_max_id': dest_max_id}
        except Exception as e:
            print(f"Skipping table {table} due to error: {e}")
            continue

    return results


if __name__ == "__main__":

    # Create source and destination connections
    source_conn = connect_to_db(source_server, source_db_user, source_db_password, source_database)
    dest_conn = connect_to_db(dest_server, dest_db_user, dest_db_password, dest_database)


    # Read table names from file
    with open('tables.txt', "r") as f:
        tables = [line.strip() for line in f]

    # Call the function
    non_matching_ids_dict = check_non_matching_ids(source_conn, dest_conn, tables)
    
    with open("non_matching_ids.txt", "w") as f:
        for table, info in non_matching_ids_dict.items():
            output_str = f"Non-matching IDs for table {table}:\n"
            print(output_str)
            f.write(output_str)
            
            output_str = f"Source max_id: {info['source_max_id']}, Destination max_id: {info['dest_max_id']}\n"
            print(output_str)
            f.write(output_str)

            
            

    
