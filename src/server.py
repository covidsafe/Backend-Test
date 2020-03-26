import os
import pymongo

db_name = "covidsafe-db"
host = "covidsafe-db.mongo.cosmos.azure.com"
port = 10255
username = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")
args = "ssl=true&\
        retrywrites=false&\
        replicaSet=global&\
        maxIdleTimeMS=12000&\
        appName=@covidsafe-db@"

uri = f"mongodb://{username}:{password}@{host}:{port}/{db_name}?{args}"
client = pymongo.MongoClient(uri)

db = client["covidsafe-db"]
