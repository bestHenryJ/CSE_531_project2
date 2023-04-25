import json
from multiprocessing import Process, pool
from time import sleep
from concurrent import futures
import sys
import grpc
import logging
import banking_pb2_grpc
from Branch import Branch
from Customer import Customer

#configure Branch Server 
def Branch_Server(branch):
    logger = configure_logger("branch")
    port = "localhost:" + str(50000 + branch.id)
    logger.info("Starting server {}. listening on {}".format(branch.id, port))
    branch.createServer()

#set up Branch process pool
def create_Branch_process_Pool(input_load):
    branchID = []
    process_pool = [] 

    for process in input_load:
        if process["type"] == "branch":
            branchID.append(process["id"])

    # reference to https://github.com/grpc/grpc/issues/16001 about use multiprocessing to deal with concurrent problem
    for process in input_load:
        if process["type"] == "branch":
           # self.Branch_Server(Branch(process["id"], process["balance"], branchID))
            branch_process = Process(target=Branch_Server, args=(Branch(process["id"], process["balance"], branchID),))
            process_pool.append(branch_process)
            branch_process.start()
    return process_pool

#set up Customer process pool
def create_Customer_process_Pool(input_load):
    customerProcesses = []
    for process in input_load:
        if process["type"] == "customer":
            customer_process = Process(target=Customer_Server, args=(Customer(process["id"], process["events"]),))
            customerProcesses.append(customer_process)
            customer_process.start()

    # wait all stub process to finish
    for customerProcess in customerProcesses:
        customerProcess.join()

    return customerProcesses

# another option to set up rpc server not implement yet
def rpcMethod(self, request, context):
    result = pool.apply_async(someExpensiveFunction(request))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server.add_insecure_port('[::]:50051')
    server.start()

#configure Customer Server
def Customer_Server(customer):
    logger = configure_logger("Customer")
    port = "localhost:" + str(50000 + customer.id)
    logger.info(" CustomerID {} is working on port {}".format(customer.id, port))
    customer.createStub(port)
    customer.executeEvents()

#configure logger 
def configure_logger(name):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

#modify result set format and write to output.json
def output_file():
    data = json.load(open("output_tmp.json"))
    output = sorted(data, key=lambda x: x["pid"])
    events = []
    for event in data:
        for elements in event["data"]:
            events.append(elements)
    eventId = set([event["id"] for event in events])
    for id in eventId:
        result = {}
        result["eventid"] = id
        data=[]
        for event in events:
            if event["id"] == id:
                data.append({"clock": event["clock"], "name": event["name"]})
        data = sorted(data, key=lambda x: x["clock"])
        result["data"] = data
        output.append(result)
    export = json.dumps(output, indent=4)
    writeTofile = open("output.json","w")
    writeTofile.write(export)
    writeTofile.close()


if __name__ == "__main__":

    with open('input.json','r') as input_data:

        input_load = json.load (input_data)
        outputfile = open("output_tmp.json", "w")
        outputfile.write("[]")
        outputfile.close()

        customerProcesses = []

        branchProcesses = []
        branchProcesses = create_Branch_process_Pool(input_load)
        
        #let all branch processes finish configuration
        sleep(0.25)

        customerProcesses = create_Customer_process_Pool(input_load)

        sleep(5)

        output_file()
        #user prompt message
        logger = configure_logger("info")
        logger.info("Stop app by ctrl + c or wait 15 minute")

