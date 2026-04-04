import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LAUNCHER = BASE_DIR / "bench_launcher.py"
PREP = BASE_DIR / "bench_prep.py"
PLOT_B1 = BASE_DIR / "bench_plot_benchmark1.py"
RESULTS_DIR = BASE_DIR / "results"
REPORTS_DIR = BASE_DIR / "reports"
CONCURRENCY_LEVELS = [1, 2, 4, 8, 16, 32]


def run_cmd(cmd):
    subprocess.run(cmd, check=True)


def load_summary(op: str, clients: int):
    path = RESULTS_DIR / f"{op}_{clients}" / "summary.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing summary file: {path}")
    with open(path) as f:
        return json.load(f)


def write_reports():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    rows = []

    for op in ("put", "get"):
        for clients in CONCURRENCY_LEVELS:
            s = load_summary(op, clients)
            rows.append({
                "op": op,
                "clients": clients,
                "duration_sec": s["duration_sec"],
                "success": s["success"],
                "errors": s["errors"],
                "throughput_ops_sec": s["throughput_ops_sec"],
                "p50_ms": s["p50_ms"],
                "p95_ms": s["p95_ms"],
                "p99_ms": s["p99_ms"],
                "target": s["target"],
            })

    csv_path = REPORTS_DIR / "benchmark1_summary.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "op",
                "clients",
                "duration_sec",
                "success",
                "errors",
                "throughput_ops_sec",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "target",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    json_path = REPORTS_DIR / "benchmark1_summary.json"
    with open(json_path, "w") as f:
        json.dump(rows, f, indent=2)

    print(f"[benchmark1] wrote report: {csv_path}", flush=True)
    print(f"[benchmark1] wrote report: {json_path}", flush=True)


def run_benchmark1(target: str, duration: int, get_key_count: int):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    for clients in CONCURRENCY_LEVELS:
        print(
            f"[benchmark1] starting PUT run: clients={clients}, duration={duration}s",
            flush=True,
        )
        run_cmd([
            sys.executable,
            str(LAUNCHER),
            "--target", target,
            "--op", "put",
            "--clients", str(clients),
            "--duration", str(duration),
            "--key-prefix", f"b1-put-c{clients}",
            "--results-dir", str(RESULTS_DIR),
        ])
        print(f"[benchmark1] finished PUT run: clients={clients}", flush=True)

    print(
        f"[benchmark1] starting GET preload: count={get_key_count}",
        flush=True,
    )
    run_cmd([
        sys.executable,
        str(PREP),
        "--target", target,
        "--count", str(get_key_count),
        "--key-prefix", "b1-get",
        "--reset",
    ])
    print("[benchmark1] finished GET preload", flush=True)

    for clients in CONCURRENCY_LEVELS:
        print(
            f"[benchmark1] starting GET run: clients={clients}, duration={duration}s",
            flush=True,
        )
        run_cmd([
            sys.executable,
            str(LAUNCHER),
            "--target", target,
            "--op", "get",
            "--clients", str(clients),
            "--duration", str(duration),
            "--key-prefix", "b1-get",
            "--get-key-count", str(get_key_count),
            "--results-dir", str(RESULTS_DIR),
        ])
        print(f"[benchmark1] finished GET run: clients={clients}", flush=True)

    print("[benchmark1] generating reports", flush=True)
    write_reports()

    print("[benchmark1] generating plot", flush=True)
    run_cmd([sys.executable, str(PLOT_B1)])
    print("[benchmark1] wrote plots/benchmark1_throughput.png", flush=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--target", required=True, help="gRPC server target, e.g. localhost:50051")
    p.add_argument("--duration", type=int, default=30)
    p.add_argument("--get-key-count", type=int, default=50000)
    return p.parse_args()


def main():
    args = parse_args()
    run_benchmark1(
        target=args.target,
        duration=args.duration,
        get_key_count=args.get_key_count,
    )


if __name__ == "__main__":
    main()