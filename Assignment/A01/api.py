# Sabin Dheke
# Assignment 1
# CMPS 5443

# Install dependencies
# pip install -r requirements.txt

# Run Programme
# uvicorn api:fapp --reload

#############################################################################################

# pythoon package used to connect to the database
import psycopg2

# to create API in python
from fastapi import FastAPI

# create a FastAPI object
fapp = FastAPI(
    title="Earthquake API",
    description="This is an API for earthquake data",
    version="1.0"
)

# to connect to the database
connection = psycopg2.connect(
    database="Project1", user='username', password='password', host='127.0.0.1', port='5432')

# to perform sql operations on database
cursor = connection.cursor()


@fapp.get("/findAll", status_code=200)
# api to get all the earthquake data in the database
# access in browser: http://127.0.0.1:8000/findAll
def findAll():
    # get all data from the database
    cursor.execute("SELECT * FROM EARTHQUAKE")
    all_earthquake_data = cursor.fetchall()

    # save the data in a list of json objects
    all_data = []
    for record in all_earthquake_data:
        all_data.append({
            'id': record[0],
            'occurred_on': record[1],
            'latitude': record[2],
            'longitude': record[3],
            'depth': record[4],
            'magnitude': record[5],
            'calculation_method': record[6],
            'network_id': record[7],
            'place': record[8],
            'cause': record[9]
        })

    return all_data


@fapp.get("/findOne", status_code=200)
# to fetch a single earthquake data from the database
# accepted parameters: id, occurred_on, latitude, longitude, depth, magnitude, calculation_method, network_id, cause
# access in browser: http://127.0.0.1:8000/findOne?id=1
def findOne(id=None, occurred_on=None, latitude=None, longitude=None, depth=None, magnitude=None, calculation_method=None, network_id=None, cause=None):
    # variable to store the data

    # if id is passed as parameter
    if id is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE WHERE earthquake_id = %s", (id,))
        except:
            return "No data found for the given parameter"
    # if occurred_on is passed as parameter
    elif occurred_on is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE WHERE occurred_on = %s", (occurred_on,))
        except:
            return "No data found for the given parameter"
    # if latitude is passed as parameter
    elif latitude is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE WHERE latitude = %s", (latitude,))
        except:
            return "No data found for the given parameter"
    # if longitude is passed as parameter
    elif longitude is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE WHERE longitude = %s", (longitude,))
        except:
            return "No data found for the given parameter"
    # if depth is passed as parameter
    elif depth is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE WHERE depth = %s", (depth,))
        except:
            return "No data found for the given parameter"
    # if magnitude is passed as parameter
    elif magnitude is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE WHERE magnitude = %s", (magnitude,))
        except:
            return "No data found for the given parameter"
    # if calculation_method is passed as parameter
    elif calculation_method is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE WHERE calculation_method = %s", (calculation_method,))
        except:
            return "No data found for the given parameter"
    # if network_id is passed as parameter
    elif network_id is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE WHERE network_id = %s", (network_id,))
        except:
            return "No data found for the given parameter"
    # if cause is passed as parameter
    elif cause is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE WHERE cause = %s", (cause,))
        except:
            return "No data found for the given parameter"
    else:
        return "No data found for the given parameter"

    data = cursor.fetchone()
    result = {
        'id': data[0],
        'occurred_on': data[1],
        'latitude': data[2],
        'longitude': data[3],
        'depth': data[4],
        'magnitude': data[5],
        'calculation_method': data[6],
        'network_id': data[7],
        'place': data[8],
        'cause': data[9]
    }
    return result


@fapp.get("/findClosest", status_code=200)
# to find the closest data from the database (based on latitude and longitude)
# access in browser: http://127.0.0.1:8000/findClosest?latitude=28%20&longitude=-99
def findClosest(latitude=None, longitude=None):
    # if latitude and longitude is passed as parameter
    if latitude is not None and longitude is not None:
        try:
            cursor.execute(
                "SELECT * FROM EARTHQUAKE ORDER BY (latitude - %s)^2 + (longitude - %s)^2 LIMIT 1", (latitude, longitude))
            data = cursor.fetchone()
            json_data = {
                'id': data[0],
                'occurred_on': data[1],
                'latitude': data[2],
                'longitude': data[3],
                'depth': data[4],
                'magnitude': data[5],
                'calculation_method': data[6],
                'network_id': data[7],
                'place': data[8],
                'cause': data[9]
            }
            return json_data
        except:
            return "No data found for the given parameter"
    else:
        return "Invalid parameter"
