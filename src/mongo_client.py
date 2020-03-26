"""
MongoDB client endpoint
"""

import os
import pymongo


DB_NAME = "covidsafe-db"
HOST = "covidsafe-db.mongo.cosmos.azure.com"
PORT = 10255
USERNAME = os.getenv("MONGO_USERNAME")
PASSWORD = os.getenv("MONGO_PASSWORD")
ARGS = "ssl=true&\
        retrywrites=false&\
        replicaSet=global&\
        maxIdleTimeMS=12000&\
        appName=@covidsafe-db@"

URI = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}?{ARGS}"


def start_client():
    """Sends mongodb queries"""
    client = pymongo.MongoClient(URI)
    for db in client.list_databases():
        print(db)


if __name__ == "__main__":
    start_client()
