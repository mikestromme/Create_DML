import pyodbc
import random
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
server = os.getenv("db_server")
db_user = os.getenv("azure_db_user")
db_password = os.getenv("azure_db_password")


# Establish a connection to your SQL Server
# Connect to the appropriate db using ODBC Data Source
driver = '{SQL Server}'
server = server
database = 'Forefront'
username = db_user
password = db_password
connection_string = 'driver=%s; server=%s; database=%s; uid=%s; pwd=%s' % (driver, server, database, username, password)
conn = pyodbc.connect(connection_string)

cursor = conn.cursor()



# Read tables from the file
with open('tables_test.txt', 'r') as file:
    tables = [line.strip() for line in file]

# Define the table for which you want to generate test data
#table_name = 'CR_CHANGE_ORDER_HEADER_MC'

# Fetch column names and data types
with open('insert_statements_test.sql', 'w') as output_file:
    for table in tables:
        cursor.execute(f'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?', table)
        columns = cursor.fetchall()

        # Generate test values
        test_values = []

        for column in columns:
            column_name, data_type = column
            if column_name == 'Company_Code':
                test_values.append("'MJS'")  # Set Company_Code to 'SUB'
            elif column_name == 'Job_Number':
                test_values.append("'12345678'")  
            elif column_name == 'Change_Order_Number':
                test_values.append("'87654321'")   
            elif column_name == 'Customer_Code':
                test_values.append("'100000'")     
            elif data_type == 'int':
                test_values.append(str(random.randint(0, 100)))
            elif data_type == 'datetime':
                test_values.append('GETDATE()')
            elif data_type == 'decimal':
                random_decimal = round(random.uniform(0, 1000), 2)
                test_values.append(str(random_decimal))
            else:
                test_values.append("'Z'")  # Default string value if data type is not recognized

        # Create the INSERT statement
        insert_statement = f'INSERT INTO Forefront.dbo.{table} ({", ".join(column[0] for column in columns)}) VALUES ({", ".join(test_values)})'

        print(insert_statement)

        output_file.write(insert_statement + '\n')

# Close connections
cursor.close()
conn.close()
