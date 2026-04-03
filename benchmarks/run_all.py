import argparse
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LAUNCHER = BASE_DIR / "bench_launcher.py"
PREP = BASE_DIR / "bench_prep.py"

CONCURRENCY_LEVELS = [1, 2, 4, 8, 16, 32]


def run_cmd(cmd):
    print("\n" + "=" * 80)
    print("RUNNING:")
    print(" ".join(cmd))
    print("=" * 80)
    subprocess.run(cmd, check=True)


def run_benchmark1(target: str, duration: int, get_key_count: int):
    # PUT runs
    for clients in CONCURRENCY_LEVELS:
        run_cmd([
            sys.executable,
            str(LAUNCHER),
            "--target", target,
            "--op", "put",
            "--clients", str(clients),
            "--duration", str(duration),
            "--key-prefix", f"b1-put-c{clients}",
        ])

    # preload once for all GET runs
    run_cmd([
        sys.executable,
        str(PREP),
        "--target", target,
        "--count", str(get_key_count),
        "--key-prefix", "b1-get",
        "--reset",
    ])

    # GET runs
    for clients in CONCURRENCY_LEVELS:
        run_cmd([
            sys.executable,
            str(LAUNCHER),
            "--target", target,
            "--op", "get",
            "--clients", str(clients),
            "--duration", str(duration),
            "--key-prefix", "b1-get",
            "--get-key-count", str(get_key_count),
        ])


def run_benchmark2(target: str, duration: int):
    # This script runs one config at a time.
    # You run it separately for:
    # 1-node, 2-node, 3-node
    run_cmd([
        sys.executable,
        str(LAUNCHER),
        "--target", target,
        "--op", "put",
        "--clients", "8",
        "--duration", str(duration),
        "--key-prefix", "b2-put",
        "--results-dir", "benchmarks/results_b2",
    ])


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--target", required=True, help="gRPC server target, e.g. localhost:50051")
    p.add_argument("--duration", type=int, default=30)
    p.add_argument("--get-key-count", type=int, default=50000)
    p.add_argument("--which", choices=["b1", "b2", "all"], default="all")
    return p.parse_args()


def main():
    args = parse_args()

    if args.which in ("b1", "all"):
        run_benchmark1(
            target=args.target,
            duration=args.duration,
            get_key_count=args.get_key_count,
        )

    if args.which in ("b2", "all"):
        run_benchmark2(
            target=args.target,
            duration=args.duration,
        )


if __name__ == "__main__":
    main()