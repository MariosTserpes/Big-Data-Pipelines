#Libaries
import pandas as pd

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pymongo import MongoClient

#A Function that will read the data from MongoDB and from collection format will be converted into a dataframe
def read_mongo(host = None, port = None, user_name = None, password = None, DB_name = "Northwind", collection = None):
    
    mongo_URI = f"mongodb://{host}:{port}/{DB_name}.{collection}"
        
    #Connect to MongoDB
    connection = MongoClient(mongo_URI)
    db = conn[DB_name]
    
    #Select all records from the collection
    cursor = db[collection].find()
    
    #Create DataFrame
    df = pd.DataFrame(list(cursor))
    
    return df
