import argparse
import os
import socket
import threading
import json
import time


def parse_args() -> argparse.Namespace:
    """Parse CLI args.

    Required flags:
    - --proto tcp|udp
    - --bind
    - --port
    - --payload-bytes
    - --requests
    - --clients
    - --log
    """
    p = argparse.ArgumentParser(description="TCP/UDP echo server for benchmarking")
    p.add_argument("--proto", choices=["tcp", "udp"], required=True)
    p.add_argument("--bind", default="0.0.0.0")
    p.add_argument("--port", type=int, default=5001)
    p.add_argument("--payload-bytes", type=int, default=1)
    p.add_argument("--requests", type=int, default=1)
    p.add_argument("--clients", type=int, default=1)
    p.add_argument("--log", required=True)
    return p.parse_args()






def main() -> None:
    """Entry point."""

    args = parse_args()
    if args.proto == "tcp":
        run_tcp_server(args.bind, args.port, args.log, args.payload_bytes, args.requests, args.clients)
    else:
        run_udp_server(args.bind, args.port, args.log, args.payload_bytes, args.requests, args.clients)
    pass


if __name__ == "__main__":
    main()