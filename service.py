
import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2
import utils
from readerwriterlock import rwlock
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class ObjectStoreServicer(pb_grpc.ObjectStoreServicer):
    """---------------------------------------------------------------------------
    Service definition
    ---------------------------------------------------------------------------

    """
    def __init__(self, ip:str, nodes:list):
        #stats
        self.store = {}
        self.puts = 0
        self.gets = 0
        self.deletes = 0
        self.updates = 0

        #server info
        self.ip = ip
        self.primary = nodes[0]
        self.cluster = [n for n in nodes if n != self.ip]
        self.is_primary = True if ip == self.primary else False

        #store locks
        self.rwlock = rwlock.RWLockWrite()
        self.rlock = self.rwlock.gen_rlock() #reader lock, allows multiple readers
        self.wlock = self.rwlock.gen_wlock() #writer lock, only one writer at a time allowed

        #stats lock
        self.stats_lock = threading.Lock()


    def fan_out(self, op_type, key="", value=b"", timeout=2.0) -> int:

        write_op = pb.WriteOp(
            type=op_type,
            key=key,
            value=value
        )

        def send_to_replica(node: str) -> bool:
            try:
                with grpc.insecure_channel(node) as channel:
                    stub = pb_grpc.ObjectStoreStub(channel)
                    stub.ApplyWrite(write_op, timeout=timeout)
                    return True
            except grpc.RpcError as e:
                return False
            except Exception as e:
                return False

        replica_count = len(self.cluster)
        if replica_count == 0:
            return 1

        total_nodes = replica_count + 1          # replicas + primary
        majority = (total_nodes +1) //  2         # total votes needed
    
        ack_count = 1
        finished = 0

        with ThreadPoolExecutor(max_workers=replica_count) as executor:
            future_to_node = {
                executor.submit(send_to_replica, node): node
                for node in self.cluster
            }

            for future in as_completed(future_to_node):
                finished += 1

                try:
                    if future.result():
                        ack_count += 1
                except Exception as e:
                    node = future_to_node[future]
                    print(f"[fan_out] future for {node} crashed: {e}")

                # Case 1: already have enough ACKs for majority
                if ack_count >= majority:
                    break

                # Case 2: even if every remaining replica succeeds, majority is impossible
                remaining = replica_count - finished
                if ack_count + remaining < majority:
                    break
        return ack_count


    #Process Put requests
    def Put(self, request, context):

        #if this node is not primary
        if not self.is_primary:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f'Error: Contact the main server for this request: {self.primary}')
            return empty_pb2.Empty() 

        # print(f"Received Put request: {request}")
        
        # Validate key and value format
        if not utils.validate_key(request.key) or not utils.validate_value(request.value):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Error: Invalid key format or value size')
            return empty_pb2.Empty()
        
        #get writers lock
        with self.wlock:
            # Check if key already exists
            if request.key in self.store:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details('Error: Key already exists')
                return empty_pb2.Empty()
            
            self.store[request.key] = request.value

        majority = (len(self.cluster) +2)//2
        n=1
        if len(self.cluster) >0:
            n = self.fan_out(pb.PUT, request.key, request.value)

        if n >= majority:
            #get stats lock to update stats
            with self.stats_lock:
                self.puts += 1
            return empty_pb2.Empty()
        else:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f'Error: System not avaliable')
            return empty_pb2.Empty() 

    #Process Get requests
    def Get(self, request, context):

        # print(f"Received Get request: {request}")

        # Validate key format
        if not utils.validate_key(request.key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Error: Invalid key format')
            return pb.GetResponse()
        
        #get reader lock to look up key
        with self.rlock:
            # Check if key exists
            if request.key not in self.store:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Error: Key not found')
                return pb.GetResponse() 
            value = self.store.get(request.key)
        
        #get lock to update stats
        with self.stats_lock:    
            self.gets += 1
        return pb.GetResponse(value=value)
  

    #Process Delete requests
    def Delete(self, request, context):

        # print(f"Received Delete request: {request}")

        #Makes sure this is the primary node
        if not self.is_primary:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f'Error: Contact the main server for this request: {self.primary}')
            return empty_pb2.Empty() 

        # Validate key format
        if not utils.validate_key(request.key):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Error: Invalid key format')
            return empty_pb2.Empty()
        
        #get writer lock
        with self.wlock:
            # Check if key exists
            if request.key not in self.store.keys():
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Error: Key not found')
                return empty_pb2.Empty()
            self.store.pop(request.key, None)

        majority = (len(self.cluster) +2)//2
        n=1

        if len(self.cluster) >0:
            n = self.fan_out(pb.DELETE, request.key)

        if n >= majority:
            #get stats lock to update stats
            with self.stats_lock:
                self.deletes += 1
            return empty_pb2.Empty()
        else:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f'Error: System is not avaliable')
            return empty_pb2.Empty() 

    #Process Update requests    
    def Update(self, request, context):
        # print(f"Received Update request: {request}")

        #make sure this is the primary node
        if not self.is_primary:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f'Error: Contact the main server for this request: {self.primary}')
            return empty_pb2.Empty() 

        # Validate key and value format
        if not utils.validate_key(request.key) or not utils.validate_value(request.value):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Error: Invalid key format or value size')
            return empty_pb2.Empty()   
        
        #get writer lock
        with self.wlock:
            # Check if key already exists
            if request.key not in self.store:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Error: Key not found')
                return empty_pb2.Empty()
            self.store[request.key] = request.value
                
        majority = (len(self.cluster) +2)//2
        n=1
        if len(self.cluster) >0:
            n = self.fan_out(pb.UPDATE, request.key, request.value)

        if n >= majority:
            #get stats lock to update stats
            with self.stats_lock:
                self.updates += 1
            return empty_pb2.Empty()
        else:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f'Error: System is not avaliable')
            return empty_pb2.Empty() 


    #Process List requests
    def List(self, request, context):

        # print(f"Received List request")
        with self.rlock:
            entries = [pb.ListEntry(key=key, size_bytes=len(value)) for key, value in self.store.items()]
        return pb.ListResponse(entries=entries)

    #Process Reset requests
    def Reset(self, request, context):

        #makes sure this is the primary node
        if not self.is_primary:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f'Error: Contact the main server for this request: {self.primary}')
            return empty_pb2.Empty() 
        
        if len(self.cluster) >0:
            self.fan_out(pb.RESET)

        # print(f"Received Reset request")
        with self.stats_lock:
            self.store.clear() 
            self.puts = 0
            self.gets = 0
            self.deletes = 0
            self.updates = 0

        return empty_pb2.Empty()


    #Process Stats requests
    def Stats(self, request, context):

        # print(f"\nReceived Stats request")
        with self.stats_lock:
            res = pb.StatsResponse(
                live_objects=self.store.__len__(),
                total_bytes=sum(len(value) for value in self.store.values()),
                puts=self.puts,
                gets=self.gets,
                deletes=self.deletes,
                updates=self.updates,
            )

        return res


    #Process ApplyWrite requests
    def ApplyWrite(self, request, context):
        """Intra-cluster RPC: primary -> replicas only.
        Clients must never call this directly.
        """
        match request.type:
            case pb.PUT:
                with self.wlock:
                    self.store[request.key] = request.value

            case pb.UPDATE:
                with self.wlock:
                    # Check if key already exists
                    if request.key in self.store:
                        self.store[request.key] = request.value
                    
            case pb.DELETE:
                with self.wlock:
                    self.store.pop(request.key, None)
                    
            case pb.RESET:
                with self.stats_lock:
                    self.store.clear()
                    self.gets = 0
                    self.puts = 0
                    self.updates = 0
                    self.deletes = 0
                
            case _:
                pass
        
        return empty_pb2.Empty()


