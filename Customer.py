from time import sleep
import logging
import grpc
import sys
import banking_pb2_grpc
from banking_pb2 import MsgRequest

class Customer:
    def __init__(self, id, events):
        self.clock = 1
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None

    # TODO: students are expected to create the Customer stub
    def createStub(self, port):
        self.stub = banking_pb2_grpc.BankStub(grpc.insecure_channel(port))

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self):
        for event in self.events:
            
            self.clock += 1
            # skip query operation which is not effecting logical clock changing and not generating result 
            if event["interface"] == "query":
               # self.executeQuery(event, 3)
               continue

            # extract customer request from events
            request = MsgRequest(id=event["id"], interface=event["interface"], money=event["money"], type="customer", clock=self.clock)
            
            response = self.stub.MsgDelivery(request)
            
            logger = self.configure_logger("name")
            logger.info("id {} is success finish {}".format(event["id"], event["interface"]))

            message = {"interface": response.interface, "result": response.result}

            # add new result to recvMsg list
            self.recvMsg.append(message)


    def executeQuery(self, event, time):

        sleep(time)

        logger = self.configure_logger("query")
        logger.info("id {} is querying".format(event["id"]))

        # send new "query" requests to finish query process
        request = MsgRequest(id=event["id"], interface="query", money=event["money"], type="customer")

        response = self.stub.MsgDelivery(request)

        message = {"interface": response.interface, "result": response.result, "money": response.money}

        self.recvMsg.append(message)

        # create output.txt file
        output = {"id":self.id,"recv":self.recvMsg}
        writeTofile = open("output.txt","a")
        writeTofile.write(str(output)+"\n")
        writeTofile.close()

    def configure_logger(self, name):
        logger = logging.getLogger(name)
        handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

