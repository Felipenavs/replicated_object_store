from concurrent import futures
import utils 
import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from service import ObjectStoreServicer

def main() -> None:

    args = utils.parse_args()
    nodes = utils.get_servers(args.cluster)
    self_node = args.listen.strip().lower()

    if nodes is None or self_node not in nodes:
        print("Error: Invalid --cluster argument. Each server must be in the format <host>:<port>")
        return

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_ObjectStoreServicer_to_server(ObjectStoreServicer(self_node, nodes), server)
    server.add_insecure_port(args.listen)
    print(f"Server listening on {args.listen}")
    server.start()
    print("Server started.")
   
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.stop(1)



if __name__ == "__main__":
    main()