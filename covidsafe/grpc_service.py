from concurrent import futures
from proto.covidsafe_pb2 import *
from proto.covidsafe_pb2_grpc import *

import grpc
import pymongo
import mongo_config
import logging

client = pymongo.MongoClient(mongo_config.URI)
database = client.database

class GrpcService(CovidSafeServerServicer):
    """grpc service to handle client rpcs"""

    def registerUser(self, request, context):
        print("Called registerUser")
        try:
            collection = database.registration
            user = {
                    "phone": request.phone,
                    "key": request.key
                   }
            collection.insert_one(user)
            return Registered(success=True)
        except Exception as e:
            print(e)
            return Registered(success=False)

    def sendInfectedLogs(self, request_iterator, context):
        collection = database.blt_logs
        for log in request_iterator:
            print(log.type)
            if log.type == Log.LogType.BLT:
                record = {
                          "timestamp": log.timestamp.ToJsonString(),
                          "uuid": log.bltResult.uuid.decode(),
                          "blt_name": log.bltResult.name
                         }
                collection.insert_one(record)
            elif log.type == Log.LogType.GPS:
                record = {
                          "timestamp": log.timestamp.ToJsonString(),
                          "latitude": log.GPSCoordinate.latitude,
                          "longitude": log.GPSCoordinate.longitude
                         }
                collection.insert_one(record)
            else:
                print

        return AddedLogs(success=True)

    def getBLTContactLogs(self, request_iterator, context):
        for key in request_iterator:
            pass
        yield BLTContactLog()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    add_CovidSafeServerServicer_to_server(GrpcService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
