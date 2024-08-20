# terminal: wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/INSTRUCTOR.csv
#install pandas: python3.11 -m pip install pandas

# import libraries
import sqlite3
import pandas as pd
#  create and connect your process to a new database STAFF
conn = sqlite3.connect('STAFF.db')

#create and load the table

table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']


#Reading the csv file

file_path = '/home/project/INSTRUCTOR.csv'
#names = column names of the DataFrame that will be created from the CSV file. 
df = pd.read_csv(file_path, names = attribute_list)

#Loading data to a table

df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')


#Running basic queries 

query_statement1 = f"SELECT * FROM {table_name}"
query_output1 = pd.read_sql(query_statement1, conn)
print(query_statement1)
print(query_output1)

query_statement2 = f"SELECT FNAME FROM {table_name}"
query_output2 = pd.read_sql(query_statement2, conn)
print(query_statement2)
print(query_output2)

query_statement3 = f"SELECT COUNT(*) FROM {table_name}"
query_output3 = pd.read_sql(query_statement3, conn)
print(query_statement3)
print(query_output3)

#append the data

data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)


#close the connection to the database

conn.close()