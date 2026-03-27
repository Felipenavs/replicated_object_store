import sys
import grpc
import random
import socket
import threading
import subprocess
from google.protobuf import empty_pb2
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc


THREADS = 20
OPS_PER_THREAD = 200
KEYS = [f"k{i}" for i in range(5)]  # overlapping keys

#---- helpers -----

#Gets a random free port number
def get_free_port():
    s = socket.socket()
    s.bind(("localhost", 0))
    port = s.getsockname()[1]
    s.close()
    return port

#starts the server
def start_server():
    port = get_free_port()
    addr = f"localhost:{port}"

    proc = subprocess.Popen(
        [sys.executable, "../server.py", "--listen", addr, "--cluster", addr],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # wait for server
    channel = grpc.insecure_channel(addr)
    grpc.channel_ready_future(channel).result(timeout=5)

    return proc, addr


# ---------- test ----------

def test_concurrency():
    proc, addr = start_server()

    try:
        channel = grpc.insecure_channel(addr)
        stub = pb_grpc.ObjectStoreStub(channel)

        stub.Reset(empty_pb2.Empty())

        # shared tracking
        lock = threading.Lock()
        barrier = threading.Barrier(THREADS)

        expected = {}
        values_seen = {k: set() for k in KEYS}

        puts = 0
        gets = 0
        deletes = 0

        errors = []

        def worker(tid):
            nonlocal puts, gets, deletes

            local_stub = pb_grpc.ObjectStoreStub(
                grpc.insecure_channel(addr)
            )

            rng = random.Random(tid)

            try:
                barrier.wait()

                for i in range(OPS_PER_THREAD):
                    key = rng.choice(KEYS)
                    op = rng.choice(["put", "get", "delete"])

                    value = f"{tid}-{i}".encode()

                    try:
                        if op == "put":
                            local_stub.Put(pb.PutRequest(key=key, value=value))

                            with lock:
                                puts += 1
                                expected[key] = value
                                values_seen[key].add(value)

                        elif op == "get":
                            resp = local_stub.Get(pb.GetRequest(key=key))
                            with lock:
                                gets += 1                                

                        else:  # delete
                            local_stub.Delete(pb.DeleteRequest(key=key))

                            with lock:
                                deletes += 1
                                expected.pop(key, None)

                    except grpc.RpcError as e:
                        if e.code() in (
                            grpc.StatusCode.ALREADY_EXISTS,
                            grpc.StatusCode.NOT_FOUND,
                        ):
                            continue
                        raise

            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(THREADS)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if errors:
            raise errors[0]

        # ---- verify results ----- 

        stub = pb_grpc.ObjectStoreStub(grpc.insecure_channel(addr))

        list_resp = stub.List(empty_pb2.Empty())
        stats = stub.Stats(empty_pb2.Empty())

        actual = {}

        for entry in list_resp.entries:
            val = stub.Get(pb.GetRequest(key=entry.key)).value
            actual[entry.key] = val

            # size must match
            assert entry.size_bytes == len(val)

        # keys match
        assert set(actual.keys()) == set(expected.keys())

        # values match
        for k in expected:
            assert actual[k] == expected[k]

        # stats match
        assert stats.puts == puts
        assert stats.gets == gets
        assert stats.deletes == deletes

        # totals match
        assert stats.live_objects == len(expected)

        total_bytes = sum(len(v) for v in expected.values())
        assert stats.total_bytes == total_bytes

        print("Concurrency test passed")

    finally:
        proc.terminate()

if __name__ == "__main__":
    test_concurrency()