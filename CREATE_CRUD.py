import pyodbc
import random
from datetime import datetime
import os
from dotenv import load_dotenv
import binascii

start_time = datetime.now()

load_dotenv()
server = os.getenv("db_server")
db_user = os.getenv("azure_db_user")
db_password = os.getenv("azure_db_password")
database = os.getenv("database")

random_company_code = 'STR'


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
with open('tables_test.txt', 'r') as file:
    tables = [line.strip() for line in file]

# Create output files for both INSERT and DELETE statements
with open('CRUD Files\\insert_statements_test.sql', 'w') as insert_output_file, open('CRUD Files\\delete_statements_test.sql', 'w') as delete_output_file \
    , open('CRUD Files\\update_statements_test.sql', 'w') as update_output_file, open('CRUD Files\\select_statements_test.sql', 'w') as select_output_file:
    #select_output_file.write('-- each row should show 1 column with the value Z after the insert and U after the update' + '\n')
    for table in tables:
        cursor.execute(f'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?', table)        
        columns = cursor.fetchall()

        # Find the primary key columns
        cursor.execute(f"SELECT column_name FROM information_schema.key_column_usage WHERE constraint_name = 'PK_{table}'")               
        pk_columns = [row[0] for row in cursor.fetchall()]

        # get pkeys where constraint name doesn't match the table name
        if len(pk_columns) == 0:    
            cursor.execute(f"SELECT column_name FROM information_schema.key_column_usage WHERE table_name = '{table}'")         
            pk_columns = [row[0] for row in cursor.fetchall()]

        # Generate test values
        test_values = []
        unique_fields = {}

        # Filter out identity columns
        columns = [(col_name, col_type) for col_name, col_type in columns if not cursor.execute(f"SELECT COLUMNPROPERTY(object_id(?), ?, 'IsIdentity')", (table, col_name)).fetchone()[0]]

        for column in columns:
            column_name, data_type = column
            if column_name in pk_columns:
                if data_type == 'int':
                    test_value = str(random.randint(0, 100))
                elif data_type == 'bigint':
                    test_value = str(random.randint(0, 100000))
                elif data_type == 'datetime':
                    test_value = 'CAST(GETDATE() AS DATE)'
                elif data_type == 'decimal':
                    test_value = str(round(random.uniform(0, 1000), 2))
                elif data_type == 'varchar' and (column_name == 'Transaction_Date' or column_name == 'Period_End_Date'): # added for triggers TRG_INS_UPD_JC_QUANTITY_HISTORY_MC and some other one
                    test_value = "'20231026'"
                else:
                    if column_name == 'Company_Code':
                        test_value = "'" + random_company_code + "'"
                    elif column_name == 'Job_Number':
                        test_value = "'12345678'"
                    else:
                        test_value = "'Z'" # f"'{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}'"
                unique_fields[column_name] = test_value
            elif column_name == 'Company_Code':
                test_value = "'" + random_company_code + "'"
            elif column_name == 'Job_Number':
                test_value = "'12345678'"
            elif column_name == 'Change_Order_Number':
                test_value = "'87654321'"
            elif column_name == 'Customer_Code':
                test_value = "'100000'"
            
            
            else:
                if data_type == 'datetime':
                    test_value = 'CAST(GETDATE() AS DATE)' 
                elif data_type == 'decimal':
                    test_value = str(round(random.uniform(0, 1000), 2))
                elif data_type == 'bigint':
                    test_value = str(random.randint(0, 100000))
                elif data_type == 'int':
                    test_value = str(random.randint(0, 100))
                elif data_type == 'uniqueidentifier':
                    test_value = "NEWID()"
                elif data_type == 'varbinary':  
                    test_value = f"0x{binascii.hexlify(bytes([random.randint(0, 100)])).decode('utf-8').upper()}"
                


                else:
                    test_value = "'Z'"

            test_values.append(test_value)

        # Create the INSERT statement
        insert_statement = f'INSERT INTO Forefront.dbo.{table} ({", ".join(column[0] for column in columns)}) VALUES ({", ".join(test_values)})'
        print(insert_statement)
        insert_output_file.write(insert_statement + '\n')


        # Generate updated test values (you can modify this part to suit your needs)
        updated_values = [f"{column[0]} = 'U'" if column[1] == 'varchar' else f"{column[0]} = {str(random.randint(101, 200))}" for column in columns]
        
        

        # Create the DELETE statement
        if unique_fields:  # Only create DELETE, Update and select if we have unique (Primary Key) fields
            delete_conditions = [f"{key} = {value}" for key, value in unique_fields.items()]
            delete_statement = f"DELETE FROM Forefront.dbo.{table} WHERE {' AND '.join(delete_conditions)}"
            delete_output_file.write(delete_statement + '\n')

            # Pick the first non-key field for updating, if available
            non_key_field = next((col for col in columns if col[0] not in unique_fields), None)
            if non_key_field:
                column_name, data_type = non_key_field
                if data_type == 'int':
                    updated_value = str(random.randint(101, 200))
                elif data_type == 'varchar':
                    updated_value = "'U'"
                elif data_type == 'decimal':
                    updated_value = 999.99 #str(random.randint(101, 200))
                elif data_type == 'datetime':
                    updated_value = 'CAST(GETDATE() as DATE)'
                else:
                    updated_value = "'U'"
            else:
                updated_value = 'M'
            update_statement = f"UPDATE Forefront.dbo.{table} SET {column_name} = {updated_value} WHERE {' AND '.join(delete_conditions)}"
            print(update_statement)
            update_output_file.write(update_statement + '\n')

            print(delete_statement)

            # Create the SELECT statement to check the updated row
            select_statement = f"SELECT {column_name} FROM Forefront.dbo.{table} WHERE {' AND '.join(delete_conditions)}"
            print(select_statement)
            select_output_file.write(select_statement + '\n')
        else: 
            #delete_output_file.write(f"-- No unique fields found for table {table}, skipping DELETE statement.\n")
            #update_output_file.write(f"-- No unique fields found for table {table}, skipping UPDATE statement.\n")
            #select_output_file.write(f"-- No unique fields found for table {table}, skipping SELECT statement.\n")

            
            non_unique_values = {}

            # Store values for the first 3 columns that were inserted (or however many you deem necessary)
            first_few_values = test_values[:3]
            non_unique_values[table] = first_few_values

            # Generate UPDATE Statement
            # first_value = first_few_values[0]
            # update_statement = f"UPDATE TOP (1) Forefront.dbo.{table} SET {columns[0][0]} = {first_value} WHERE {conditions}"
            # update_output_file.write(update_statement + '\n')
            
            first_few_columns = ", ".join(col[0] for col in columns[:3])  # Choose the first 3 columns as example
            conditions = " AND ".join(f"{col[0]} = {val}" for col, val in zip(columns[:3], first_few_values))

            # Generate DELETE Statement
            delete_statement = f"DELETE TOP (1) FROM Forefront.dbo.{table} WHERE {conditions}"
            delete_output_file.write(delete_statement + '\n')

            # Generate UPDATE Statement
            first_value = first_few_values[0]
            update_statement = f"UPDATE TOP (1) Forefront.dbo.{table} SET {columns[0][0]} = {first_value} WHERE {conditions}"
            update_output_file.write(update_statement + '\n')

            # Generate SELECT Statement
            select_statement = f"SELECT TOP (1) * FROM Forefront.dbo.{table} WHERE {conditions}"
            select_output_file.write(select_statement + '\n')
            


# Define the names of the files to read and the output file
input_files = ['CRUD Files\\insert_statements_test.sql', 'CRUD Files\\update_statements_test.sql', 'CRUD Files\\delete_statements_test.sql']
output_file = 'CRUD Files\\CRUD.sql'

# Open the output file for writing
with open(output_file, 'w') as output:
    # Iterate through each input file
    for file_name in input_files:
        # Open the input file for reading
        with open(file_name, 'r') as f:
            # Read content and write it to the output file
            content = f.read()

            if file_name == 'CRUD Files\\update_statements_test.sql' or file_name == 'CRUD Files\\delete_statements_test.sql':
                output.write('/*\n\n')
                output.write(content)
                output.write('\n*/')
            else:
                output.write(content)
                
        # Add newlines between each file content
        output.write('\n\n')

# Close connections
cursor.close()
conn.close()


stop_time = datetime.now()
duration = stop_time - start_time

print('\n\n\n')
print(f"Start Time: {start_time}")
print(f"Stop Time: {stop_time}")
print(f"Duration: {duration}")
