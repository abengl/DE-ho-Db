""""
Consider a dataset of employee records that is available with an HR team in a CSV file. As a Data Engineer, you are required to create the database called STAFF and load the contents of the CSV file as a table called INSTRUCTORS. The headers of the available data are: ID, FNAME, LNAME, CITY, and CCODE
"""

import sqlite3
import pandas as pd

# Create and connect to an new db STAFF
conn = sqlite3.connect('STAFF.db')

# Create and load the table
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY','CCODE']

table_name_2 = 'Departments'
attribute_list_2 = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']

# Read the csv file
file_path = '/home/aben/Documents/IBM/Course3/W2/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names=attribute_list)

file_path_2 = '/home/aben/Documents/IBM/Course3/W2/Departments.csv'
df2 = pd.read_csv(file_path_2, names=attribute_list_2)

# Load the table
df.to_sql(table_name, conn, if_exists='replace', index=False)
print('Table 1 is ready')

df2.to_sql(table_name_2, conn, if_exists='replace', index=False)
print('Table 2 is ready')
        
# Basic queries

# Viewing all the data in the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Viewing only FNAME column of data
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Viewing the total number of entries in the table
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#  Appending some data to the table
data_dict = {
    'ID': [100],
    'FNAME': ['John'],
    'LNAME': ['Doe'],
    'CITY': ['Paris'],
    'CCODE': ['FR']
}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists='append', index=False)
print('Data appended successfully')

data_dict_2 = {
    'DEPT_ID': [9],
    'DEP_NAME': ['Quality Assurance'],
    'MANAGER_ID': [30010],
    'LOC_ID': ['L0010']
}

# Viewing the total number of entries in the table
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT * FROM {table_name_2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT DEP_NAME FROM {table_name_2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) FROM {table_name_2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


# Close the connection to the database after all the queries are executed.
conn.close()