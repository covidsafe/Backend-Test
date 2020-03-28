from concurrent import futures
from proto.covidsafe_pb2 import *
from proto.covidsafe_pb2_grpc import *

import grpc
import pymongo
import mongo_config
import logging

# set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

# set up connection to mongoDB
client = pymongo.MongoClient(mongo_config.URI)
database = client.database

# grpc service handler
class GrpcService(CovidSafeServerServicer):
    """grpc service to handle client rpcs"""

    def registerUser(self, request, context):
        logger.debug("Received registerUser() with ({phone},{key})".format(
            phone=request.phone,
            key=request.key))
        try:
            collection = database.registration
            user = {
                    "phone": request.phone,
                    "key": request.key
                   }
            collection.insert_one(user)
            return Registered(success=True)
        except Exception as e:
            logger.error(e)
            return Registered(success=False)

    def sendInfectedLogs(self, request_iterator, context):
        logger.debug("Received sendInfectedLogs()")
        collection = database.blt_logs
        for log in request_iterator:
            print(log.type)
            if log.type == Log.LogType.BLT:
                record = {
                          "timestamp": log.timestamp,
                          "uuid": log.bltResult.uuid.decode(),
                          "blt_name": log.bltResult.name
                         }
                collection.insert_one(record)
            elif log.type == Log.LogType.GPS:
                record = {
                          "timestamp": log.timestamp,
                          "latitude": log.GPSCoordinate.latitude,
                          "longitude": log.GPSCoordinate.longitude
                         }
                collection.insert_one(record)
            else:
                logger.error("Unknown LogType in sendInfectedLogs()")
        return AddedLogs(success=True)

    def getBLTContactLogs(self, request_iterator, context):
        logger.debug("Received getBLTContactLogs()")
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
