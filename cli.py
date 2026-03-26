import utils
import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2
from collections import deque 
import argparse
import shlex


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

        # command, *obj = line.strip().split(" ")
        # command = command.lower()

        parts = shlex.split(line)
        command, *obj = parts
        command = command.lower()

        match command:

            #Put
            case "put":

                #no key or value provided
                if len(obj) < 2:
                    print("Error: Invalid number of arguments. Put command requires: Put <key> <value>")
                    continue

                key = obj[0]
                value = " ".join(obj[1:])  
                try:
                    response = primary_stub.Put(pb.PutRequest(key=key, value=value.encode('utf-8')), timeout=2.0)
                    print(f"Success: Saved object with key: {key} and value: {value}")
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}") 
                    

            #Get
            case "get":

                if(len(obj) < 1):
                    print("Error: Invalid number of arguments. Get command requires: Get <key>")
                    continue

                key = obj[0]
                value = " ".join(obj[1:])
                stub = utils.next_read_stub(stubs)
                try:
                    response = stub.Get(pb.GetRequest(key=key), timeout=2.0)
                    print(f"Success: value = {response.value.decode("utf-8")}")
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}")

            #Update
            case "update":

                #no key or value provided
                if len(obj) < 2:
                    print("Error: Invalid number of arguments. Put command requires: Put <key> <value>")
                    continue

                key = obj[0]
                value = " ".join(obj[1:])
                try:
                    response = primary_stub.Update(pb.UpdateRequest(key=key, value=value.encode('utf-8')))
                    print(f"Success: Updated object with key: {key} to value: {value}")
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}")

            #Delete
            case "delete":

                if(len(obj) < 1):
                    print("Error: Invalid number of arguments. Get command requires: Get <key>")
                    continue

                key = obj[0]
                value = " ".join(obj[1:])
                try:
                    response = primary_stub.Delete(pb.DeleteRequest(key=key), timeout=2.0)
                    print(f"Success: Deleted key {key}")         
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}")

            #List
            case "list":
                stub = utils.next_read_stub(stubs)
                try:
                    response = stub.List(empty_pb2.Empty(), timeout=2.0)
                    print("Success: OK")
                    print(response)
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}")
                
            
            #Stats
            case "stats":
                
                stub = utils.next_read_stub(stubs)
                try:   
                    response= stub.Stats(empty_pb2.Empty(), timeout=2.0)
                    print("Success: OK")
                    print(response)
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}")
                
            #Reset
            case "reset":
                try:
                    response = primary_stub.Reset(empty_pb2.Empty(), timeout=2.0)
                    print("Success: OK")        
                except grpc.RpcError as e:
                    print(f"Error: {e.code().name}. {e.details()}")
                
            #Exit
            case "exit" | "quit" | "q":
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