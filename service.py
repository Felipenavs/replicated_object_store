
import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2
import utils
from readerwriterlock import rwlock
import threading



"""
---------------------------------------------------------------------------
Service implementation logic
---------------------------------------------------------------------------
"""
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

        #locks
        self.rwlock = rwlock.RWLockWrite()
        self.rlock = self.rwlock.gen_rlock() #reader lock, allows multiple readers
        self.wlock = self.rwlock.gen_wlock() #writer lock, only one writer at a time allowed

        self.stats_lock = threading.Lock() # used to increment counters

    def Put(self, request, context):

        md = dict(context.invocation_metadata())

        if not self.primary and md != "primary":
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f'Contact the main server for this request: {self.primary}')
            return empty_pb2.Empty() 

        # print(f"Received Put request: {request}")
        
        # Validate key and value format
        if not utils.validate_key(request.key) or not utils.validate_value(request.value):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid key or value')
            return empty_pb2.Empty()
        
        #get writers lock
        with self.wlock:
            # Check if key already exists
            if request.key in self.store:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details('Key already exists')
                return empty_pb2.Empty()
                
            self.store[request.key] = request.value

        #get stats lock to update stats
        with self.stats_lock:
            self.puts += 1
        return empty_pb2.Empty()

    def Get(self, request, context):

        # print(f"Received Get request: {request}")

        # Validate key format
        if not utils.validate_key(request.key):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Invalid key')
            return pb.GetResponse()
        
        #get reader lock to look up key
        with self.rlock:
            # Check if key exists
            if request.key not in self.store:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Key not found')
                return pb.GetResponse() 
            value = self.store.get(request.key)
        
        #get lock to update stats
        with self.stats_lock:    
            self.gets += 1
        return pb.GetResponse(value=value)
  

    #
    def Delete(self, request, context):

        # print(f"Received Delete request: {request}")

        # Validate key format
        if not utils.validate_key(request.key):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Invalid key')
            return empty_pb2.Empty()
        
        #get writer lock
        with self.wlock:
            # Check if key exists
            value = self.store.pop(request.key, None)
            if value is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Key not found')
                return empty_pb2.Empty()

        #get stats lock
        with self.stats_lock:
            self.deletes += 1
        return empty_pb2.Empty()

    
    def Update(self, request, context):


        # print(f"Received Update request: {request}")

        # Validate key and value format
        if not utils.validate_key(request.key) or not utils.validate_value(request.value):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid key or value')
            return empty_pb2.Empty()   
        
        #get writer lock
        with self.wlock:
            # Check if key already exists
            if request.key not in self.store:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Key not found')
                return empty_pb2.Empty()
                
            self.store[request.key] = request.value
        
        #update stats
        with self.stats_lock:
            self.updates += 1
        return empty_pb2.Empty()



    def List(self, request, context):

        # print(f"Received List request")
        with self.rlock:
            entries = [pb.ListEntry(key=key, size_bytes=len(value)) for key, value in self.store.items()]
        return pb.ListResponse(entries=entries)

    def Reset(self, request, context):

        # print(f"Received Reset request")
        with self.wlock:
            self.store.clear()
            self.puts = 0
            self.gets = 0
            self.deletes = 0
            self.updates = 0
        return empty_pb2.Empty()

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

    def ApplyWrite(self, request, context):
        """Intra-cluster RPC: primary -> replicas only.
        Clients must never call this directly.
        """

        match request.type:
            case pb.PUT:
                pass
            case pb.UPDATE:
                pass
            case pb.Delete:
                pass
            case pb.Reset:
                pass
            case _:
                pass


        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

