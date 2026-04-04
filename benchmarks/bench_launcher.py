import argparse
import json
import shutil
import subprocess
import sys
import time
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


def terminate_all(proc_entries):
    for entry in proc_entries:
        proc = entry["proc"]
        if proc.poll() is None:
            proc.terminate()

    time.sleep(1)

    for entry in proc_entries:
        proc = entry["proc"]
        if proc.poll() is None:
            proc.kill()


def main():
    args = parse_args()
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    run_dir = results_dir / f"{args.op}_{args.clients}"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)

    print(
        f"[launcher] starting run: op={args.op}, clients={args.clients}, "
        f"target={args.target}, duration={args.duration}s",
        flush=True,
    )

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

        proc = subprocess.Popen(cmd)
        procs.append({
            "worker_id": worker_id,
            "proc": proc,
            "out_file": out_file,
        })

    while True:
        all_done = True

        for entry in procs:
            rc = entry["proc"].poll()

            if rc is None:
                all_done = False
                continue

            if rc != 0:
                worker_id = entry["worker_id"]
                print(
                    f"[launcher] FAIL FAST: worker {worker_id} exited with code {rc}. "
                    f"Stopping benchmark.",
                    file=sys.stderr,
                    flush=True,
                )
                terminate_all(procs)
                raise RuntimeError(f"Worker {worker_id} exited with code {rc}")

        if all_done:
            break

        time.sleep(0.2)

    total_success = 0
    total_errors = 0
    all_latencies = []
    elapsed_values = []

    for path in sorted(run_dir.glob("worker_*.json")):
        with open(path) as f:
            data = json.load(f)

        if data.get("failed"):
            raise RuntimeError(
                f"Worker {data.get('worker_id')} failed: "
                f"{data.get('error_type')} "
                f"{data.get('error_code', data.get('error_message', ''))}"
            )

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

    print(f"[launcher] completed run: op={args.op}, clients={args.clients}", flush=True)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
