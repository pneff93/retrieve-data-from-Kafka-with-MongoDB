from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://user:password@mongo:27017")
db = client["TruckData"]
col = db["truck_1"]

data = pd.DataFrame(col.find())
pipeline = [
    {
        '$match': {
            'timestamp': {
                '$gte': '2022-01-07'
            }
        }
    }
]

df = pd.DataFrame(col.aggregate(pipeline))
df