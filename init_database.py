# connect to mongodb and insert data

import pymongo
import datetime
import random
import time
from pathlib import Path
import json

# connect to mongodb
client = pymongo.MongoClient("mongodb+srv://kumariamrita1803:hbS9XLuRmQEwUdx0@clustercovid19.dawy2l9.mongodb.net/?retryWrites=true&w=majority")
db = client["COVID_2019"]

for json_file in Path("Sample").glob("*.json"):
    print(json_file)
    collection = db[json_file.stem]
    with open(json_file) as f:
        data = json.load(f)
        collection.insert_many(data)
    time.sleep(1)
    