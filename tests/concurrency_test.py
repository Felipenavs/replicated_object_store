import sys
from pathlib import Path
import grpc
import random
import socket
import threading
import subprocess
from google.protobuf import empty_pb2


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc


THREADS = 20
OPS_PER_THREAD = 200
KEYS = [f"k{i}" for i in range(5)] ## number of overlapping keys


def get_free_port():
    s = socket.socket()
    s.bind(("localhost", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_server():
    port = get_free_port()
    addr = f"localhost:{port}"
    server_path = PROJECT_ROOT / "server.py"

    proc = subprocess.Popen(
        [sys.executable, str(server_path), "--listen", addr, "--cluster", addr],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=str(PROJECT_ROOT),
    )

    channel = grpc.insecure_channel(addr)
    grpc.channel_ready_future(channel).result(timeout=5)

    return proc, addr


def run_concurrency_test():
    proc, addr = start_server()

    try:
        stub = pb_grpc.ObjectStoreStub(grpc.insecure_channel(addr))
        stub.Reset(empty_pb2.Empty(), timeout=5.0)

        lock = threading.Lock()
        barrier = threading.Barrier(THREADS)

        expected = {}
        puts = gets = deletes = 0
        errors = []

        def worker(tid):
            nonlocal puts, gets, deletes

            stub = pb_grpc.ObjectStoreStub(grpc.insecure_channel(addr))
            rng = random.Random(tid)

            try:
                barrier.wait()

                for i in range(OPS_PER_THREAD):
                    key = rng.choice(KEYS)
                    op = rng.choice(["put", "get", "delete"])
                    value = f"{tid}-{i}".encode()

                    try:
                        if op == "put":
                            stub.Put(pb.PutRequest(key=key, value=value), timeout=5.0)
                            with lock:
                                puts += 1
                                expected[key] = value

                        elif op == "get":
                            stub.Get(pb.GetRequest(key=key), timeout=5.0)
                            with lock:
                                gets += 1

                        else:
                            stub.Delete(pb.DeleteRequest(key=key), timeout=5.0)
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

        # ---- verification ----
        stub = pb_grpc.ObjectStoreStub(grpc.insecure_channel(addr))

        list_resp = stub.List(empty_pb2.Empty(), timeout=5.0)
        stats = stub.Stats(empty_pb2.Empty(), timeout=5.0)

        actual = {}

        for entry in list_resp.entries:
            val = stub.Get(pb.GetRequest(key=entry.key), timeout=5.0).value
            actual[entry.key] = val
            assert entry.size_bytes == len(val)

        assert set(actual.keys()) == set(expected.keys())

        for k in expected:
            assert actual[k] == expected[k]

        assert stats.puts == puts
        assert stats.gets == gets
        assert stats.deletes == deletes
        assert stats.live_objects == len(expected)

        total_bytes = sum(len(v) for v in expected.values())
        assert stats.total_bytes == total_bytes

        print("Concurrency test passed")

    finally:
        proc.terminate()
        proc.wait(timeout=5)


if __name__ == "__main__":
    run_concurrency_test()