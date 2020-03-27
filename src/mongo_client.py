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
ARGS = "ssl=true&" \
       "retrywrites=false&" \
       "maxIdleTimeMS=12000&" \
       "appName=@covidsafe-db@"

URI = "mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}?{ARGS}".format(
    USERNAME=USERNAME,
    PASSWORD=PASSWORD,
    HOST=HOST,
    PORT=PORT,
    DB_NAME=DB_NAME,
    ARGS=ARGS
)


def start_client():
    """Sends mongodb queries"""
    client = pymongo.MongoClient(URI)
    for database in client.list_databases():
        print(database)


if __name__ == "__main__":
    start_client()
