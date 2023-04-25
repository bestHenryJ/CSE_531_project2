from concurrent import futures
from time import sleep
import json
import grpc
import logging
import sys
import banking_pb2_grpc
from banking_pb2 import MsgRequest, MsgResponse


class Branch(banking_pb2_grpc.BankServicer):
    def __init__(self, id, balance, branches):
        self.clock = 1
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # iterate the processID of the branches
        self.events = list()

         # TODO: students are expected to store the processID of the branches
        # pass

    # TODO: students are expected to process requests from both Client and Branch
    def createServer(self):

        #configure grpc server for each branch 
        port = "localhost:" + str(50000 + self.id)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=6))
        banking_pb2_grpc.add_BankServicer_to_server(self, server)
        server.add_insecure_port(port)
        server.start()

        #create stubList
        for branchId in self.branches:
            if branchId != self.id:
                port = "localhost:" + str(50000 + branchId)
                self.stubList.append(banking_pb2_grpc.BankStub(grpc.insecure_channel(port)))

        #sleep id seconds Prevents file write conflicts and intialize output file 
        sleep(self.id)
        array = json.load(open("output_tmp.json"))
        array.append(self.output())
        output = json.dumps(array, indent=4)
        writeTofile = open("output_tmp.json","w")
        writeTofile.write(output)
        writeTofile.close()

        #keep server opening to wait all transactions finish
        sleep(900)
        server.stop()

    def MsgDelivery(self, request, context):
        message = "success"
        
        #main function to implement query, deposit and withdraw task
        if request.interface == "deposit":
            self.balance += request.money
            # use type "Customer" to identify Customer request
            if request.type == "customer":
                #customer deposit event request, excute and propagate
                self.Event_Request(request)
                self.Event_Execute(request)
                self.MsgPropagate(request)
            else:
                #branch deposit event request and execute
                self.Propagate_Request(request)
                self.Propagate_Execute(request)

        elif request.interface == "withdraw":
            if self.balance >= request.money:
                self.balance -= request.money
                if request.type == "customer":
                    #customer withdraw event request, excute and propagate
                    self.Event_Request(request)
                    self.Event_Execute(request)
                    self.MsgPropagate(request)
                else:
                    #branch withdraw event request and execute
                    self.Propagate_Request(request)
                    self.Propagate_Execute(request)
            else:
                message = "no enough money"

        response = MsgResponse(id=request.id, interface=request.interface, result=message, money=self.balance, clock=self.clock)

        if request.type == "customer":
            self.Event_Response(response)

        return response

    # only use for propagating between in branches, use type "branch" to identify Branch request
    def MsgPropagate(self, request):
        for stub in self.stubList:
            response =  stub.MsgDelivery(MsgRequest(id=request.id, interface=request.interface, money=request.money, type = "branch", clock=self.clock))
            self.Propagate_Response(response)

    def Event_Request(self, request):
        localclock = self.clock
        self.clock = max(self.clock, request.clock) + 1
        event_name = request.interface + "_request"
        self.events.append({"id": request.id, "name": event_name, "clock": self.clock})
        logger = self.configure_logger('1')
        logger.info("Event {} id {} on Branch {}, logical clock change from {} to {}".format(event_name, request.id, self.id, localclock, self.clock))



    def Event_Execute(self, request):
        self.clock += 1
        event_name = request.interface + "_execute"
        self.events.append({"id": request.id, "name": event_name, "clock": self.clock})
        logger = self.configure_logger('2')
        logger.info("Event {} id {} on Branch {}, logical clock is {}".format(event_name, request.id, self.id, self.clock))



    def Propagate_Request(self, request):
        localclock = self.clock
        self.clock = max(self.clock, request.clock) + 1
        event_name = request.interface + "_propagate_request"
        self.events.append({"id": request.id, "name": event_name, "clock": self.clock})
        logger = self.configure_logger('3')
        logger.info("Event {} id {} on Branch {}, logical clock change from {} to {}".format(event_name, request.id, self.id, localclock, self.clock))



    def Propagate_Execute(self, request):
        self.clock += 1
        event_name = request.interface + "_propagate_execute"
        self.events.append({"id": request.id, "name": event_name, "clock": self.clock})
        logger = self.configure_logger('4')
        logger.info("Event {} id {} on Branch {}, logical clock is {}".format(event_name, request.id, self.id, self.clock))


    def Propagate_Response(self, response):
        localclock = self.clock
        self.clock = max(self.clock, response.clock) + 1
        event_name = response.interface + "_propagate_response" 
        self.events.append({"id": response.id, "name": event_name, "clock": self.clock})
        logger = self.configure_logger('5')
        logger.info("Event {} id {} on Branch {}, logical clock change from {} to {}".format(event_name, response.id, self.id, localclock, self.clock))


    def Event_Response(self, response):
        self.clock += 1
        event_name = response.interface + "_response"
        self.events.append({"id": response.id, "name": event_name, "clock": self.clock})
        logger = self.configure_logger('6')
        logger.info("Event {} id {} on Branch {}, logical clock is {}".format(event_name, response.id, self.id, self.clock))


    def output(self):
        output = {}
        output["pid"] = self.id
        output["data"] = self.events
        return output

    def configure_logger(self, name):
        logger = logging.getLogger(name)
        handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger
