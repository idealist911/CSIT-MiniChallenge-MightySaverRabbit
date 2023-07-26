from fastapi import FastAPI, Response, HTTPException
from pandas import DataFrame
import json
import dateutil
from pymongo import MongoClient
import certifi
from bson import json_util
import datetime

def get_database():
    # Provide mongodb atlas url
    DATABASE_URL = "mongodb+srv://userReadOnly:7ZT817O8ejDfhnBM@minichallenge.q4nve1r.mongodb.net/"
    
    # Use certifi package for TLS certificate for MongoClient to have secure communication with mongodb
    ca = certifi.where()
    
    # Create a connection to the url
    client = MongoClient(DATABASE_URL, tlsCAFile=ca)

    # Return database
    return client['minichallenge']

# MAIN #
app = FastAPI()
db = get_database()

# ROUTES #
@app.get("/flight")
def flight(departureDate: str|None=None, returnDate: str|None=None, destination: str|None=None):
    # Retrieve a collection from database
    collection = db['flights']
    
    if (departureDate==None or returnDate==None or destination==None):
        raise HTTPException(status_code=400, detail="Bad input")
    if (date_checker(departureDate)==False or date_checker(returnDate)==False):
        raise HTTPException(status_code=400, detail="Bad input")
    
    departureDate_parsed = dateutil.parser.parse(departureDate)
    returnDate_parsed = dateutil.parser.parse(returnDate)
    if (returnDate_parsed < departureDate_parsed):
        raise HTTPException(status_code=400, detail="Bad input")
    else:
        dbResults = collection.find({
            "date": {"$eq": departureDate_parsed}, 
            "destcity": destination, 
            "srccity": "Singapore"
        })
        dbResults_df = DataFrame(dbResults)
        #print(dbResults_df.to_dict('records'))
        
        results = []
        for dbResult in dbResults_df.to_dict('records'):
            #print(dbResult)
            result = flight_helper(dbResult, returnDate_parsed)
            results.append(result)
            
        # identify the cheapest price
        cheapestPrice = -1
        for result in results:
            price = result['Departure Price'] + result['Return Price']
            if (cheapestPrice == -1):
                cheapestPrice = price
            elif (price < cheapestPrice):
                cheapestPrice = price
        
        # log the results with the cheapest price
        cheapest = []
        for result in results:
            price = result['Departure Price'] + result['Return Price']
            if (price == cheapestPrice):
                cheapest.append(result)
    
    return json.loads(json_util.dumps(cheapest))
    
@app.get("/hotel")
def hotel(checkInDate: str|None=None, checkOutDate: str|None=None, destination: str|None=None):
    # Retrieve a collection from database
    collection = db['hotels']
    
    if (checkInDate==None or checkOutDate==None or destination==None):
        raise HTTPException(status_code=400, detail="Bad input")
    if (date_checker(checkInDate)==False or date_checker(checkOutDate)==False):
        raise HTTPException(status_code=400, detail="Bad input")
    
    checkInDate_parsed = dateutil.parser.parse(checkInDate)
    checkOutDate_parsed = dateutil.parser.parse(checkOutDate)
    if (checkOutDate_parsed < checkInDate_parsed):
        raise HTTPException(status_code=400, detail="Bad input")
    else:
        dbResults = collection.find({
            "date": {"$eq": checkInDate_parsed}, 
            "city": destination
        })
        dbResults_df = DataFrame(dbResults)
        #print(dbResults_df.to_dict('records'))
        
        results = []
        for dbResult in dbResults_df.to_dict('records'):
            #print(dbResult)
            result = hotel_helper(dbResult, checkOutDate_parsed)
            results.append(result)
        
        # identify the cheapest price
        cheapestPrice = -1
        for result in results:
            price = result['Price']
            if (cheapestPrice == -1):
                cheapestPrice = price
            elif (price < cheapestPrice):
                cheapestPrice = price
        
        # log the results with the cheapest price
        cheapest = []
        for result in results:
            price = result['Price']
            if (price == cheapestPrice):
                cheapest.append(result)
    
    return json.loads(json_util.dumps(cheapest))
    
    
# HELPER FUNCTIONS #
def flight_helper(flight, returnDate_parsed) -> dict:
    collection = db['flights']
    returnFlight_result = collection.find({
        "date": {"$eq": returnDate_parsed}, 
        "srccity": flight["destcity"],
        "airlinename": flight["airlinename"],
        "destcity": "Singapore"
    })
    returnFlight_df = DataFrame(returnFlight_result)
    #print(returnFlight_df.to_dict('records'))
    returnFlight = returnFlight_df.to_dict('records')[0]
    return {
        "City": flight["destcity"],
        "Departure Date": flight["date"].isoformat()[:-9],
        "Departure Airline": flight["airlinename"],
        "Departure Price": flight["price"],
        "Return Date": returnFlight["date"].isoformat()[:-9],
        "Return Airline": returnFlight["airlinename"],
        "Return Price": returnFlight["price"]
    }
    
def hotel_helper(hotel, checkOutDate_parsed) -> dict:
    collection = db['hotels']
    hotel_results = collection.find({
        "city": hotel['city'],
        "date": {
            "$gte": hotel['date'],
            "$lte": checkOutDate_parsed
        }, 
        "hotelName": hotel["hotelName"]
    })
    price_total = 0
    for result in hotel_results:
        price_total += result['price']
    
    return {
        "City": hotel["city"],
        "Check In Date": hotel["date"].isoformat()[:-9],
        "Check Out Date": checkOutDate_parsed,
        "Hotel": hotel["hotelName"],
        "Price": price_total
    }
    
def date_checker(date_string):
    valid = True
    date_format = '%Y-%m-%d'
    try:
        #valid = bool(dateutil.parser.parse(date))
        return bool(datetime.datetime.strptime(date_string, date_format))
    except ValueError:
        print("Incorrect data format, should be YYYY-MM-DD")
        return False
    
    return valid
    