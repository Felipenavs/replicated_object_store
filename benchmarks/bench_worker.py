import argparse
import json
import random
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc

PAYLOAD_SIZE = 4096  # 4 KiB


def make_payload(size=PAYLOAD_SIZE) -> bytes:
    return b"x" * size


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    idx = int(p * (len(sorted_vals) - 1))
    return sorted_vals[idx]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--target", required=True, help="host:port to send RPCs to")
    p.add_argument("--op", required=True, choices=["put", "get"])
    p.add_argument("--duration", type=float, default=30.0)
    p.add_argument("--worker-id", type=int, required=True)
    p.add_argument("--key-prefix", required=True)
    p.add_argument("--get-key-count", type=int, default=0)
    p.add_argument("--out", required=True)
    return p.parse_args()


def write_result(path, result):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(result, f)


def main():
    args = parse_args()

    latencies_ms = []
    success = 0
    errors = 0

    payload = make_payload()

    channel = grpc.insecure_channel(args.target)
    grpc.channel_ready_future(channel).result(timeout=5)
    stub = pb_grpc.ObjectStoreStub(channel)

    rng = random.Random(args.worker_id)
    start = time.perf_counter()
    deadline = start + args.duration

    put_counter = 0

    while time.perf_counter() < deadline:
        try:
            t0 = time.perf_counter()

            if args.op == "put":
                key = f"{args.key_prefix}-w{args.worker_id}-{put_counter}"
                stub.Put(pb.PutRequest(key=key, value=payload), timeout=5.0)
                put_counter += 1
            else:
                if args.get_key_count <= 0:
                    raise ValueError("--get-key-count must be > 0 for get workloads")
                idx = rng.randrange(args.get_key_count)
                key = f"{args.key_prefix}-pre-{idx}"
                stub.Get(pb.GetRequest(key=key), timeout=5.0)

            dt_ms = (time.perf_counter() - t0) * 1000.0
            latencies_ms.append(dt_ms)
            success += 1

        except grpc.RpcError as e:
            errors += 1
            elapsed = time.perf_counter() - start

            print(
                f"[worker {args.worker_id}] FAIL FAST: grpc error "
                f"code={e.code()} details={e.details()}",
                file=sys.stderr,
                flush=True,
            )

            latencies_ms.sort()
            result = {
                "worker_id": args.worker_id,
                "op": args.op,
                "target": args.target,
                "elapsed_sec": elapsed,
                "success": success,
                "errors": errors,
                "p50_ms": percentile(latencies_ms, 0.50),
                "p95_ms": percentile(latencies_ms, 0.95),
                "p99_ms": percentile(latencies_ms, 0.99),
                "latencies_ms": latencies_ms,
                "failed": True,
                "error_type": "grpc",
                "error_code": str(e.code()),
                "error_details": e.details(),
            }
            write_result(args.out, result)
            sys.exit(1)

        except Exception as e:
            errors += 1
            elapsed = time.perf_counter() - start

            print(
                f"[worker {args.worker_id}] FAIL FAST: {type(e).__name__}: {e}",
                file=sys.stderr,
                flush=True,
            )

            latencies_ms.sort()
            result = {
                "worker_id": args.worker_id,
                "op": args.op,
                "target": args.target,
                "elapsed_sec": elapsed,
                "success": success,
                "errors": errors,
                "p50_ms": percentile(latencies_ms, 0.50),
                "p95_ms": percentile(latencies_ms, 0.95),
                "p99_ms": percentile(latencies_ms, 0.99),
                "latencies_ms": latencies_ms,
                "failed": True,
                "error_type": type(e).__name__,
                "error_message": str(e),
            }
            write_result(args.out, result)
            sys.exit(1)

    elapsed = time.perf_counter() - start
    latencies_ms.sort()

    result = {
        "worker_id": args.worker_id,
        "op": args.op,
        "target": args.target,
        "elapsed_sec": elapsed,
        "success": success,
        "errors": errors,
        "p50_ms": percentile(latencies_ms, 0.50),
        "p95_ms": percentile(latencies_ms, 0.95),
        "p99_ms": percentile(latencies_ms, 0.99),
        "latencies_ms": latencies_ms,
        "failed": False,
    }

    write_result(args.out, result)


if __name__ == "__main__":
    main()
