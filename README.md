# Replicated Object Store

## Overview

This project implements a distributed in-memory object store in Python using gRPC. It supports:

* a **replicated storage server**
* a **REST proxy**
* a **CLI proxy**
* **benchmark scripts**
* a **concurrent correctness test**

The server can run either as a **single node** or as a **primary + replicas** cluster. In cluster mode, the primary handles all writes and propagates them to replicas using `ApplyWrite`. Reads can be served by any node.

## Features

* In-memory key-value object store
* gRPC API based on `objectstore.proto`
* Replication with majority acknowledgment
* REST interface for HTTP clients
* CLI interface for manual testing
* Benchmark scripts for throughput and replication cost
* Concurrent correctness test

## Project Structure

```text
.
├── server.py
├── restproxy.py
├── cli.py
├── objectstore.proto
├── objectstore_pb2.py
├── objectstore_pb2_grpc.py
├── requirements.txt
├── test/
│   └── concurrency_test.py
└── benchmarks/
    ├── bench_worker.py
    ├── bench_launcher.py
    ├── bench_prep.py
    ├── benchmark1.py
    ├── benchmark2.py
    ├── bench_report_benchmark1.py
    └── bench_report_benchmark2.py
```

## Requirements

* Python 3.10+
* `pip`
* gRPC Python packages

Install dependencies with:

```bash
pip install -r requirements.txt
```

If needed, regenerate the gRPC stubs with:

```bash
python3 -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --grpc_python_out=. \
  objectstore.proto
```

## Running the Server

### Single-node mode

Run one standalone server:

```bash
python3 server.py --listen localhost:50051 --cluster localhost:50051
```

### Multi-node cluster

Example 2-node cluster:

```bash
python3 server.py --listen localhost:50051 --cluster localhost:50051,localhost:50052
python3 server.py --listen localhost:50052 --cluster localhost:50051,localhost:50052
```

Example 3-node cluster:

```bash
python3 server.py --listen localhost:50051 --cluster localhost:50051,localhost:50052,localhost:50053
python3 server.py --listen localhost:50052 --cluster localhost:50051,localhost:50052,localhost:50053
python3 server.py --listen localhost:50053 --cluster localhost:50051,localhost:50052,localhost:50053
```

The primary is chosen automatically as the lexicographically smallest endpoint in the cluster list.

## Running the CLI Proxy

The CLI can be used for manual testing.

```bash
python3 cli.py --cluster localhost:50051
```

Example commands:

```text
put mykey hello
get mykey
delete mykey
update mykey newvalue
list
reset
stats
```

You can also pipe commands from a file:

```bash
python3 cli.py --cluster localhost:50051 < testcmds.txt
```

## Running the REST Proxy

Start the REST proxy with:

```bash
python3 restproxy.py --cluster localhost:50051 --port 8080
```

Example requests:

```bash
curl -X PUT http://localhost:8080/objects/mykey --data-binary 'hello'
curl http://localhost:8080/objects/mykey
curl http://localhost:8080/objects
curl http://localhost:8080/stats
curl -X DELETE http://localhost:8080/objects/mykey
```

## Running the Concurrent Correctness Test

The concurrent test starts a temporary single-node server and launches multiple threads that perform mixed operations on overlapping keys.

Run it with:

```bash
python3 test/concurrency_test.py
```

What it checks:

* concurrent `put`, `get`, and `delete` operations
* correctness of the final key-value state
* correctness of the server stats counters
* no corrupted or missing values under contention

## Running Benchmark 1

Benchmark 1 measures **throughput vs. concurrency** for `put` and `get` using 4 KiB objects.

Run:

```bash
python3 benchmarks/benchmark1.py --target localhost:50051 --duration 30 --get-key-count 1000
```

This benchmark runs workloads at concurrency levels:

* 1
* 2
* 4
* 8
* 16
* 32

Then generate the report/plots:

```bash
python3 benchmarks/bench_report_benchmark1.py
```

## Running Benchmark 2

Benchmark 2 measures the **cost of synchronous replication** using an 8-client sustained `put` workload with 4 KiB objects.

It should be run under three configurations:

* single standalone server
* 2-node cluster
* 3-node cluster

Example runs:

### Single node

```bash
python3 benchmarks/benchmark2.py --target localhost:50051 --mode single
```

### 2-node cluster

```bash
python3 benchmarks/benchmark2.py --target localhost:50051 --mode two-node
```

### 3-node cluster

```bash
python3 benchmarks/benchmark2.py --target localhost:50051 --mode three-node
```

Then generate the report/table:

```bash
python3 benchmarks/bench_report_benchmark2.py
```

## Notes


* When running benchmarks on iLab machines, you must **add your NetID** to the scripts wherever required (e.g., SSH/remote execution commands), otherwise the scripts will not run correctly.

* The **concurrent correctness test is run locally** and does not require any cluster setup. You can run it with:

```bash
python3 test/concurrency_test.py
```

* Make sure the target server or cluster is already running before starting the CLI, REST proxy, or benchmarks(if you are not running them with the sh scripts).

* In replicated mode, replicas reject direct client writes with `FAILED_PRECONDITION`.

* Benchmark 1 uses a server thread pool of 32 to match the maximum client concurrency level.

## Summary

This project demonstrates:

* RPC-based service design with gRPC
* concurrent server correctness
* replicated writes with majority acknowledgment
* performance benchmarking under load
* multiple client-facing interfaces (CLI and REST)
