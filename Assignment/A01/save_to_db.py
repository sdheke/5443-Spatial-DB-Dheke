# Sabin Dheke
# Assignment 1
# CMPS 5443

# Install dependencies
# pip install -r requirements.txt

# Run Programme
# python save_to_db.py
# python3 save_to_db.py

# Total execution time: 5.657538174000365 seconds
# Total records saved in database: 23119

#############################################################################################

# pythoon package used to connect to the database
import psycopg2

# for using regular expressions
import re

# for recording the program execution time
import timeit

# start timer of program execution
start = timeit.default_timer()

# to connect to the database
conn = psycopg2.connect(database="Project1", user='username',
                        password='password', host='127.0.0.1', port='5432')

# to perform sql operations on database
cursor = conn.cursor()

# Create a table in the database (if it does not already exist)
cursor.execute('''
CREATE TABLE IF NOT EXISTS EARTHQUAKE (
    earthquake_id TEXT PRIMARY KEY NOT NULL, occurred_on TEXT, latitude NUMERIC,
    longitude NUMERIC, depth NUMERIC, magnitude NUMERIC, calculation_method TEXT,
    network_id TEXT, place TEXT, cause TEXT
)
''')

# to commit the changes to the database
conn.commit()

# data file path
filename = 'earthquakes.csv'

# open file in read mode
file = open(filename, 'r')

# records counter
counter = 0

# read the file line by line and insert the data into the database
for row in file:
    # increase the counter
    counter += 1
    print("Counter => ", counter)

    # to skip the first line (because it contains the column names)
    if counter != 1:
        # split the line into columns but ignore the commas inside the double quotes
        # (because 'place' column contains commas inside the double quotes)
        values = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', row)

        # remove the double quotes
        # because values contains place column data in double quotes (e.g. "hii there")
        # if we save it as it is, it will be saved as "hii there" (with double quotes)
        # then when we fetch the data from the database, it will be fetched as ""hii there"" (with double double quotes)
        values = [value.replace('"', '') for value in values]

        # insert the values into the database
        cursor.execute('''
        INSERT INTO EARTHQUAKE (
            earthquake_id, 
            occurred_on, 
            latitude, 
            longitude, 
            depth, 
            magnitude, 
            calculation_method, 
            network_id, 
            place, 
            cause
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            values[0],
            values[1],
            values[2],
            values[3],
            values[4],
            values[5],
            values[6],
            values[7],
            values[8],
            values[9]
        ))

# Commit the changes to the database
# we can save the changes to the database after every row insertion
# but it will take more time to save the changes to the database
# it is the fastest way to save the changes to the database
# although it is not the safest way (if the program crashes, the changes will not be saved)
conn.commit()

# close the file
file.close()

# close database connection
conn.close()

# stop timer of program execution
stop = timeit.default_timer()
total_time = stop - start

# print the total time taken by the program to execute
print('\nTotal Time: ', total_time)
