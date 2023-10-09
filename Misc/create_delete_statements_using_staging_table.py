import pyodbc
import random
from datetime import datetime
import os
from dotenv import load_dotenv

# load environment variables
load_dotenv()
server = os.getenv("db_server")
db_user = os.getenv("azure_db_user")
db_password = os.getenv("azure_db_password")


# Establish a connection to your SQL Server
driver = '{SQL Server}'
server = server
database = 'Forefront'
username = db_user
password = db_password
connection_string = 'driver=%s; server=%s; database=%s; uid=%s; pwd=%s' % (driver, server, database, username, password)
conn = pyodbc.connect(connection_string)

cursor = conn.cursor()



# Read tables from a file
with open('tables_test.txt', 'r') as file:
    tables = [line.strip() for line in file]

# Fetch column names and data types create DML and write to file
with open('CRUD Files\\delete_statements_for_SSIS.sql', 'w') as output_file:
    for table in tables:

        # set staging table variable
        staging_table = f"Forefront_staging.dbo.{table}_DELETE"
        
        # Execute a query to retrieve columns in the primary key
        cursor.execute(f"SELECT column_name " 
                        f"FROM information_schema.key_column_usage " 
                        f"WHERE constraint_name = 'PK_{table}'")

        # Fetch the results
        primary_key_fields = [row.column_name for row in cursor.fetchall()]

        if primary_key_fields != []:

            # Print the primary key fields
            print(primary_key_fields)

            # Create a comma-separated string of primary key field names
            primary_key_fields_str = ', b.'.join(primary_key_fields) + ' = d.' + ' = d.'.join(primary_key_fields)

            # Create a list of join conditions
            join_conditions = [f"b.{field} = d.{field}" for field in primary_key_fields]

            # Join the conditions using 'AND'
            join_condition_str = " AND ".join(join_conditions)

            # create the join and delete DML
            delete_statement = f"DELETE b " \
                                f"FROM Forefront.dbo.{table} b " \
                                f"JOIN {staging_table} d " \
                                f"ON {join_condition_str}"
            
            print(delete_statement)

            # write to script file
            output_file.write(delete_statement + '\n')

        elif primary_key_fields == []:
            output_file.write('--' + table + '\n')

# Close connections
cursor.close()
conn.close()
