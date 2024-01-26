from fastapi import FastAPI
from snowflake_handler import SnowflakeHandler
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from fastapi import Body

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache

from redis import asyncio as aioredis


app = FastAPI()


@app.on_event("startup")
async def startup():
    redis = aioredis.from_url("redis://default:zZ6zRNfGhP9FRzwuk9wT6DHHhN3KhISc@redis-12726.c309.us-east-2-1.ec2.cloud.redislabs.com:12726")
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")


# Requires the PyMongo package.
# https://api.mongodb.com/python/current
load_dotenv()


try:
    mongodb_client = MongoClient(os.getenv("MONGODB_HOST_CONNECTION_STRING"))['COVID_2019']
    print(mongodb_client)
except Exception as e:
    print(f"Cannot connect to MongoDB: {e}")
    

snowflake_handler = SnowflakeHandler(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)


@app.get('/')
async def index():
    return {'message': 'This is the API of the Snowflake COVID-19 Data'}

@app.get('/cases')
@cache(expire=60)
async def cases(country: str, date: str = "2022-12-15"):
    # curl http://localhost:8000/cases?country=New%20Zealand&date=2020-12-15
    if country is None:
        return {'message': 'Please provide a country'}

    if "," in country:
        countries = country.split(",")
        country_ = "','".join(countries)
        query = f"SELECT SUM(CASES) FROM JHU_COVID_19 WHERE COUNTRY_REGION IN ('{country_}') AND DATE = '{date}' GROUP BY COUNTRY_REGION"
    else:
        query = f"SELECT SUM(CASES) FROM JHU_COVID_19 WHERE COUNTRY_REGION = '{country}' AND DATE = '{date}'"
        countries = [country]

    print(query)
    snowflake_handler.connect()
    snowflake_handler.execute(query=query)
    results = snowflake_handler.fetchall()
    snowflake_handler.close()

    response = {country: result[0] for country, result in zip(countries, results)}
    return response


@app.get('/daily-cases')
@cache(expire=60)
async def daily_cases(country: str):
    if country is None:
        return {'message': 'Please provide a country'}
    
    query = f"SELECT CASES, DATE, CASE_TYPE, DIFFERENCE FROM JHU_COVID_19 WHERE COUNTRY_REGION='{country}'  ORDER BY DATE"
    snowflake_handler.connect()
    snowflake_handler.execute(query=query)
    results = snowflake_handler.fetchall()
    snowflake_handler.close()
    
    response = [{"cases": result[0], "date": result[1], "case_type": result[2], "difference": result[3]} for result in results]
    return response

    

@app.get('/cases-lat-long')
@cache(expire=60)
async def cases_lat_long(date: str = "2022-12-15"):
    query = f"SELECT COUNTRY_REGION, LONG, LAT, SUM(CASES) FROM JHU_COVID_19 WHERE DATE = '{date}' GROUP BY COUNTRY_REGION, LONG, LAT"
    
    snowflake_handler.connect()
    snowflake_handler.execute(query=query)
    results = snowflake_handler.fetchall()
    snowflake_handler.close()
    
    response = [{"country": result[0], "long": result[1], "lat": result[2], "cases": result[3]} for result in results]
    return response



# List all Metadata Collections in MongoDB
@app.get('/metadata')
@cache(expire=60)
async def metadata():
    collections = mongodb_client["Metadata"]
    results = collections.find({})
    return list(results)

@app.get('/comments')
@cache(expire=60)
async def comments(data_id: str = None):
    if data_id is None:
        return {'message': 'Please provide a data_id'}
    
    collections = mongodb_client["Comments"]
    results = collections.find({"data_id": data_id}, {"_id": 0})
    return list(results)

# add comment
@app.post('/comments')
async def add_comment(data_id: str = Body(None), user: str = Body(None), content: str = Body(None)):
    if data_id is None:
        return {'message': 'Please provide a data_id'}
    
    if user is None:
        return {'message': 'Please provide a user'}
    
    if content is None:
        return {'message': 'Please provide a content'}
    
    print(data_id, user, content)
    collections = mongodb_client["Comments"]
    collections.insert_one({"data_id": data_id, "user": user, "content": content})
    return {'message': 'Comment added successfully'}
