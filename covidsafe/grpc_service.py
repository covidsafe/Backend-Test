from concurrent import futures
from proto.covidsafe_pb2 import *
from proto.covidsafe_pb2_grpc import *

import grpc

class GrpcService(CovidSafeServerServicer):
    def registerUser(self, request, context):
        print("Called registerUser")
        return Registered(success=False)

    def sendInfectedLogs(self, request_iterator, context):
        for log in request_iterator:
            pass
        return AddedLogs(success=False)

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
