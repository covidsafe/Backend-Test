from concurrent import futures
from enum import Enum
from proto.covidsafe_pb2 import *
from proto.covidsafe_pb2_grpc import *

import grpc
import pymongo
import mongo_config
import logging
import uuid

# set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(tag)s - %(message)s')
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

# set up connection to mongoDB
client = pymongo.MongoClient(mongo_config.URI)
database = client.database

# convenience functions
def generate_tag(name, context):
    tag = {"tag": name + ":" + context.peer() + ":" + str(uuid.uuid4())}
    return tag

def log_rpc_received(tag):
    logger.info("Received RPC", extra=tag)

def log_rpc_completed(tag, status):
    logger.info("Completed RPC with status {status}".format(status=str(status)),
            extra=tag)

def insert_records(records, collection, label):
    """Inserts records into MongoDB collection"""

    tag = {"tag": label + ":InsertRecords"}

    if isinstance(records, list):
        logger.info("Inserting record group into {collection}".format(
            collection=collection.name), extra=tag)
        for record in record:
            logger.info("Inserting record {record}".format(record=record),
            extra=tag)
        collection.insert_many(records)
    else:
        logger.info("Inserting single record {record} into {collection}".format(
            record=records, collection=collection.name), extra=tag)
        collection.insert_one(records)
    logger.info("Successfully inserted records into {collection}".format(
        collection=collection.name), extra=tag)


# Enum to track RPC completion
class RPC(Enum):
    SUCCESS = 0
    FAILURE = 1


# grpc service handler
class GrpcService(CovidSafeServerServicer):
    """grpc service to handle client rpcs"""

    def registerUser(self, request, context):
        tag = generate_tag("registerUser", context)
        log_rpc_received(tag)

        try:
            collection = database.registration
            user = {
                    "phone": request.phone,
                    "key": request.key
                   }
            insert_records(user, collection, tag["tag"])
            log_rpc_completed(tag, RPC.SUCCESS)
            return Registered(success=True)
        except Exception as e:
            logger.error("Exception: " + e, extra=tag)
            log_rpc_completed(tag, RPC.FAILURE)
            return Registered(success=False)

    def sendInfectedUserData(self, request, context):
        tag = generate_tag("sendInfectedUserData", context)
        log_rpc_received(tag)

        try:
            collection = database.infected_registration
            user = {
                    "phone": request.phone,
                    "key": request.key,
                    "dob": request.dob,
                    "name": request.name
                   }
            insert_records(user, collection, tag["tag"])
            log_rpc_completed(tag, RPC.SUCCESS)
            return UserDataSent(success=True)
        except Exception as e:
            logger.error("Exception: " + e, extra=tag)
            log_rpc_completed(tag, RPC.FAILURE)
            return UserDataSent(success=False)

    def sendInfectedLogs(self, request_iterator, context):
        tag = generate_tag("sendInfectedLogs", context)
        log_rpc_received(tag)

        # TODO:
        # Are we guaranteed to have a single type of Log requests?
        # If so, this can be optimized into insert_many()
        for log in request_iterator:
            if log.type == Log.LogType.BLT:
                try:
                    collection = database.blt_logs
                    record = {
                              "timestamp": log.timestamp,
                              "uuid": str(uuid.UUID(bytes=log.bltResult.uuid)),
                              "blt_name": log.bltResult.name
                             }
                    insert_records(record, collection, tag["tag"])
                except Exception as e:
                    logger.error("Exception: " + e, extra=tag)
                    log_rpc_completed(tag, RPC.FAILURE)
                    return AddedLogs(success=False)

            elif log.type == Log.LogType.GPS:
                try:
                    collection = database.gps_logs
                    record = {
                              "timestamp": log.timestamp,
                              "latitude": log.GPSCoordinate.latitude,
                              "longitude": log.GPSCoordinate.longitude
                             }
                    insert_records(record, collection, tag["tag"])
                except Exception as e:
                    logger.error("Exception: " + e, extra=tag)
                    log_rpc_completed(tag, RPC.FAILURE)
                    return AddedLogs(success=False)

            else:
                logger.error("Unknown LogType {type}".format(type=log.type), 
                        extra=tag)
                log_rpc_completed(tag, RPC.FAILURE)
                return AddedLogs(success=False)

        log_rpc_completed(tag, RPC.SUCCESS)
        return AddedLogs(success=True)

    # TODO: implement bucketing
    def getBLTContactLogs(self, request_iterator, context):
        try:
            tag = generate_tag("getBLTContactLogs", context)
            log_rpc_received(tag)
            collection = database.blt_logs
            # TODO: key is unused for now, till bucketing is implemented
            key = [entry.key for entry in request_iterator]
            cursor = collection.find({})
            bltContactLog = BLTContactLog()
            for log in cursor:
                bltContactLog.key.key = uuid.uuid4().bytes
                bltContactLog.uuid.append(log["uuid"].encode())
                bltContactLog.timestamp.append(log["timestamp"])
            log_rpc_completed(tag, RPC.SUCCESS)
            yield bltContactLog
        except Exception as e:
            logger.error("Exception: " + str(e), extra=tag)
            log_rpc_completed(tag, RPC.FAILURE)

# for testing
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    add_CovidSafeServerServicer_to_server(GrpcService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


# edit and only keep this version later 
def ssl_serve():
    with open("service.key", "rb") as f:
        private_key = f.read()
    with open("service.pem", "rb") as f:
        certificate_chain = f.read()

    server_credentials = grpc.ssl_server_credentials(
         ((private_key, certificate_chain,),))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    add_CovidSafeServerServicer_to_server(GrpcService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
