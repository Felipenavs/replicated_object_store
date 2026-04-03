import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

PAYLOAD_SIZE = 4096  # 4 KiB


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--target", required=True)
    p.add_argument("--count", type=int, required=True)
    p.add_argument("--key-prefix", required=True)
    p.add_argument("--reset", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    channel = grpc.insecure_channel(args.target)
    grpc.channel_ready_future(channel).result(timeout=5)
    stub = pb_grpc.ObjectStoreStub(channel)

    if args.reset:
        stub.Reset(empty_pb2.Empty(), timeout=10.0)

    payload = b"x" * PAYLOAD_SIZE

    inserted = 0
    skipped = 0

    for i in range(args.count):
        key = f"{args.key_prefix}-pre-{i}"
        try:
            stub.Put(pb.PutRequest(key=key, value=payload), timeout=5.0)
            inserted += 1
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                skipped += 1
                continue
            raise

    print(
        f"Preload complete: inserted={inserted}, skipped_existing={skipped}, "
        f"target={args.target}, prefix={args.key_prefix}"
    )


if __name__ == "__main__":
    main()