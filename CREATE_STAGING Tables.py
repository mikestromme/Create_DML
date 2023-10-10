import pyodbc
import random
from datetime import datetime
import os
from dotenv import load_dotenv
import random

load_dotenv()
server = os.getenv("db_server")
db_user = os.getenv("azure_db_user")
db_password = os.getenv("azure_db_password")
database = os.getenv("database")


# Establish a connection to your SQL Server
# Connect to the appropriate db using ODBC Data Source
driver = '{SQL Server}'
server = server
database = database
username = db_user
password = db_password
connection_string = 'driver=%s; server=%s; database=%s; uid=%s; pwd=%s' % (driver, server, database, username, password)
conn = pyodbc.connect(connection_string)

cursor = conn.cursor()


# Read tables from the file
with open('tables.txt', 'r') as file:
    tables = [line.strip() for line in file]

with open('CRUD Files\\CREATE INSERT Tables.sql', 'w') as output_file:
    for table in tables:
        cursor.execute(f'SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, sc.precision AS NUMERIC_PRECISION, sc.scale AS NUMERIC_SCALE \
                         FROM INFORMATION_SCHEMA.COLUMNS c \
                           LEFT JOIN sys.columns sc ON c.COLUMN_NAME = sc.name \
                         WHERE c.TABLE_NAME= ? AND sc.object_id = OBJECT_ID(?)' , table, table)
        columns = cursor.fetchall()

        # Construct the CREATE TABLE command based on the fetched structure.
        create_table_command = "CREATE TABLE " + table + "_INSERT ("
        for column in columns:
            column_definition = f"{column.COLUMN_NAME} {column.DATA_TYPE}"
            if column.DATA_TYPE in ['char', 'varchar', 'nchar', 'nvarchar']:
                length = 'MAX' if column.CHARACTER_MAXIMUM_LENGTH == -1 else column.CHARACTER_MAXIMUM_LENGTH
                column_definition += f"({length})"
                #column_definition += f"({column.CHARACTER_MAXIMUM_LENGTH})"
            # For decimal and numeric types
            elif column.DATA_TYPE in ['decimal', 'numeric']: #and hasattr(column, 'NUMERIC_PRECISION') and hasattr(column, 'NUMERIC_SCALE'):
                column_definition += f"({column.NUMERIC_PRECISION}, {column.NUMERIC_SCALE})"
            create_table_command += column_definition + ","
        create_table_command = create_table_command.rstrip(",") + ")"

        print(create_table_command)
        output_file.write(create_table_command + '\n')


with open('CRUD Files\\CREATE UPDATE Tables.sql', 'w') as output_file:
    for table in tables:
        cursor.execute(f'SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, sc.precision AS NUMERIC_PRECISION, sc.scale AS NUMERIC_SCALE \
                         FROM INFORMATION_SCHEMA.COLUMNS c \
                           LEFT JOIN sys.columns sc ON c.COLUMN_NAME = sc.name \
                         WHERE c.TABLE_NAME= ? AND sc.object_id = OBJECT_ID(?)' , table, table)
        columns = cursor.fetchall()

        # Construct the CREATE TABLE command based on the fetched structure.
        create_table_command = "CREATE TABLE " + table + "_UPDATE ("
        for column in columns:
            column_definition = f"{column.COLUMN_NAME} {column.DATA_TYPE}"
            if column.DATA_TYPE in ['char', 'varchar', 'nchar', 'nvarchar']:
                length = 'MAX' if column.CHARACTER_MAXIMUM_LENGTH == -1 else column.CHARACTER_MAXIMUM_LENGTH
                column_definition += f"({length})"
                #column_definition += f"({column.CHARACTER_MAXIMUM_LENGTH})"
            # For decimal and numeric types
            elif column.DATA_TYPE in ['decimal', 'numeric']: #and hasattr(column, 'NUMERIC_PRECISION') and hasattr(column, 'NUMERIC_SCALE'):
                column_definition += f"({column.NUMERIC_PRECISION}, {column.NUMERIC_SCALE})"
            create_table_command += column_definition + ","
        create_table_command = create_table_command.rstrip(",") + ")"

        print(create_table_command)
        output_file.write(create_table_command + '\n')

with open('CRUD Files\\CREATE DELETE Tables.sql', 'w') as output_file:
    for table in tables:
        cursor.execute(f'SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, sc.precision AS NUMERIC_PRECISION, sc.scale AS NUMERIC_SCALE \
                         FROM INFORMATION_SCHEMA.COLUMNS c \
                           LEFT JOIN sys.columns sc ON c.COLUMN_NAME = sc.name \
                         WHERE c.TABLE_NAME= ? AND sc.object_id = OBJECT_ID(?)' , table, table)
        columns = cursor.fetchall()

        # Construct the CREATE TABLE command based on the fetched structure.
        create_table_command = "CREATE TABLE " + table + "_DELETE ("
        for column in columns:
            column_definition = f"{column.COLUMN_NAME} {column.DATA_TYPE}"
            if column.DATA_TYPE in ['char', 'varchar', 'nchar', 'nvarchar']:
                length = 'MAX' if column.CHARACTER_MAXIMUM_LENGTH == -1 else column.CHARACTER_MAXIMUM_LENGTH
                column_definition += f"({length})"
                #column_definition += f"({column.CHARACTER_MAXIMUM_LENGTH})"
            # For decimal and numeric types
            elif column.DATA_TYPE in ['decimal', 'numeric']: #and hasattr(column, 'NUMERIC_PRECISION') and hasattr(column, 'NUMERIC_SCALE'):
                column_definition += f"({column.NUMERIC_PRECISION}, {column.NUMERIC_SCALE})"
            create_table_command += column_definition + ","
        create_table_command = create_table_command.rstrip(",") + ")"

        print(create_table_command)
        output_file.write(create_table_command + '\n')

# Execute the CREATE TABLE command.
#cursor.execute(create_table_command)
#conn.commit()

cursor.close()
conn.close()

