import json

# http://api.weatherapi.com/v1/current.json?key=&q=USA&aqi=no

from datetime import datetime
from http.client import responses

import requests
import boto3
from decimal import Decimal
#from dotenv import load_dotenv
import os

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("weather")

# def configure():
#     load_dotenv()

def get_weather_data(city):
    api_url = "http://api.weatherapi.com/v1/current.json"
    params = {
        "q" : city,
        "key" : "4a572ab364864d72bfd10804242809"
    }
    response = requests.get(api_url,params=params)
    data = response.json()
    return data

def lambda_handler(event, context):
    configure()
    cities = ["Houston","Chicago", "New york","Seattle", "Tampa", "Calgary", "Phoenix", "Atlanta", "Washington", "San Francisco" ]
    for city in cities:
        data = get_weather_data(city.replace(" ", "%"))

        temp = data['current']['temp_f']
        feelslike = data['current']['feelslike_f']
        windchill = data['current']['windchill_f']
        wind_mph = data['current']['wind_mph']
        recorded_at = data['current']['last_updated']
        humidity = data['current']['humidity']

        print(city, temp, feelslike, windchill, wind_mph, recorded_at, humidity)
        current_time = datetime.utcnow().isoformat()

        item = {
            'city': city,
            'temp': temp,
            'feelslike': feelslike,
            'windchill': windchill,
            'wind_mph': wind_mph,
            'recorded_at': recorded_at,
            'humidity': humidity,
            'current_time': str(current_time)
        }

        item = json.loads(json.dumps(item), parse_float=Decimal)
        # Insert data into DynamoDB
        table.put_item(
            Item=item
        )






