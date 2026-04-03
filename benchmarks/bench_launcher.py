import argparse
import json
import math
import os
import shutil
import subprocess
from pathlib import Path
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

BASE_DIR = Path(__file__).resolve().parent
WORKER = BASE_DIR / "bench_worker.py"

def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    idx = int(p * (len(sorted_vals) - 1))
    return sorted_vals[idx]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--target", required=True)
    p.add_argument("--op", required=True, choices=["put", "get"])
    p.add_argument("--clients", type=int, required=True)
    p.add_argument("--duration", type=float, default=30.0)
    p.add_argument("--key-prefix", required=True)
    p.add_argument("--get-key-count", type=int, default=0)
    p.add_argument("--results-dir", default="benchmarks/results")
    return p.parse_args()


def main():
    args = parse_args()
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    run_dir = results_dir / f"{args.op}_{args.clients}"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)

    procs = []
    for worker_id in range(args.clients):
        out_file = run_dir / f"worker_{worker_id}.json"
        cmd = [
            sys.executable,
            str(WORKER),
            "--target", args.target,
            "--op", args.op,
            "--duration", str(args.duration),
            "--worker-id", str(worker_id),
            "--key-prefix", args.key_prefix,
            "--out", str(out_file),
        ]

        if args.op == "get":
            cmd += ["--get-key-count", str(args.get_key_count)]

        procs.append(subprocess.Popen(cmd))

    for p in procs:
        rc = p.wait()
        if rc != 0:
            raise RuntimeError(f"Worker exited with code {rc}")

    total_success = 0
    total_errors = 0
    all_latencies = []
    elapsed_values = []

    for path in run_dir.glob("worker_*.json"):
        with open(path) as f:
            data = json.load(f)
        total_success += data["success"]
        total_errors += data["errors"]
        elapsed_values.append(data["elapsed_sec"])
        all_latencies.extend(data["latencies_ms"])

    all_latencies.sort()
    elapsed = max(elapsed_values) if elapsed_values else args.duration
    throughput = total_success / elapsed if elapsed > 0 else 0.0

    summary = {
        "op": args.op,
        "clients": args.clients,
        "target": args.target,
        "duration_sec": elapsed,
        "success": total_success,
        "errors": total_errors,
        "throughput_ops_sec": throughput,
        "p50_ms": percentile(all_latencies, 0.50),
        "p95_ms": percentile(all_latencies, 0.95),
        "p99_ms": percentile(all_latencies, 0.99),
    }

    with open(run_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()