import utils
import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2
from collections import deque 
import argparse
from google.protobuf.json_format import MessageToDict


def cli_argparse() -> list[str]:
    """Parse CLI args.

    Required flags:
    - --cluster
    """
    p = argparse.ArgumentParser(description="CLI")
    p.add_argument("--cluster", required=True, nargs="+", help="List of cluster nodes in the format <host>:<port>")
    args = p.parse_args()
    servers = utils.get_servers(args.cluster)
    return servers


def run():

    servers = cli_argparse()
    if not servers:
        print("Error: Invalid --cluster argument. Each server must be in the format <host>:<port>")
        return
    

    channels = [grpc.insecure_channel(addr) for addr in servers]
    stubs = deque([pb_grpc.ObjectStoreStub(ch) for ch in channels])
    primary_stub = stubs[0]

    while True:


        try:
            line = input("> ")
        except EOFError:
            break  # stop when file ends

        if not line:
            continue

        command, *obj = line.strip().split(" ")
        command = command.lower()
        match command:
            #Put
            case "1" | "put":

                #no key or value provided
                if len(obj) < 2:
                    print("Error: Invalid number of arguments. Put command requires: Put <key> <value>")
                    continue
                
                key, *value = obj
                value = utils.parse_value(value) 

                try:
                    response = primary_stub.Put(pb.PutRequest(key=key, value=value.encode('utf-8')))
                    print(f"Success: Saved object with key: {key} and value: {value}")
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}") 

            #Get
            case "2" | "get":

                if(len(obj) < 1):
                    print("Error: Invalid number of arguments. Get command requires: Get <key>")
                    continue

                key,*_ = obj
                stub = utils.next_read_stub(stubs)
                try:
                    response = stub.Get(pb.GetRequest(key=key))
                    print(f"Success: value = {response.value.decode("utf-8")}")
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}")

            #Update
            case "3" | "update":

                #no key or value provided
                if len(obj) < 2:
                    print("Error: Invalid number of arguments. Put command requires: Put <key> <value>")
                    continue

                key, *value = obj
                value = utils.parse_value(value) 
                try:
                    response = primary_stub.Update(pb.UpdateRequest(key=key, value=value.encode('utf-8')))
                    print(f"Success: Updated object with key: {key} to value: {value}")
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}")

            #Delete
            case "4" | "delete":

                if(len(obj) < 1):
                    print("Error: Invalid number of arguments. Get command requires: Get <key>")
                    continue
                key,*_ = obj
                try:
                    response = primary_stub.Delete(pb.DeleteRequest(key=key))
                    print(f"Success: Deleted key {key}")         
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}")

            #List
            case "5" | "list":
                stub = utils.next_read_stub(stubs)
                response = stub.List(empty_pb2.Empty())
                print("Success: OK")
                print(MessageToDict(response))
            
            #Stats
            case "6" | "stats":
                response = primary_stub.Stats(empty_pb2.Empty())
                print("Success: OK")
                print(MessageToDict(response))

            #Reset
            case "7" | "reset":
                response = primary_stub.Reset(empty_pb2.Empty())
                print("Success: OK")

            #Exit
            case "8" | "exit" | "quit" | "q":
                break
            
            #Invalid
            case _:
                print("Invalid command")

    #close channels when done
    for ch in channels:
        ch.close()
    
    print("Client finished.")

if __name__ == "__main__":
    run()