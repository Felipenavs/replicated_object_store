import argparse
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LAUNCHER = BASE_DIR / "bench_launcher.py"
REPORT_B2 = BASE_DIR / "bench_report_benchmark2.py"
REPORTS_DIR = BASE_DIR / "reports"


def run_cmd(cmd):
    subprocess.run(cmd, check=True)


def run_benchmark2(target: str, duration: int, results_dir: str, key_prefix: str):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print(
        f"[benchmark2] starting PUT run: clients=8, duration={duration}s, "
        f"results_dir={results_dir}",
        flush=True,
    )
    run_cmd([
        sys.executable,
        str(LAUNCHER),
        "--target", target,
        "--op", "put",
        "--clients", "8",
        "--duration", str(duration),
        "--key-prefix", key_prefix,
        "--results-dir", results_dir,
    ])
    print(
        f"[benchmark2] finished PUT run: clients=8, results_dir={results_dir}",
        flush=True,
    )



def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--target", required=True, help="gRPC server target, e.g. localhost:50051")
    p.add_argument("--duration", type=int, default=30)
    p.add_argument(
        "--results-dir",
        default="benchmarks/results_b2_single",
        help="Where this benchmark 2 run should be written",
    )
    p.add_argument(
        "--key-prefix",
        default="b2-put",
        help="Prefix for keys used during benchmark 2",
    )
    p.add_argument(
        "--generate-report",
        action="store_true",
        help="Generate the combined benchmark 2 reports after all configs finish",
    )
    return p.parse_args()



def main():
    args = parse_args()
    run_benchmark2(
        target=args.target,
        duration=args.duration,
        results_dir=args.results_dir,
        key_prefix=args.key_prefix,
    )

    if args.generate_report:
        print("[benchmark2] generating reports", flush=True)
        run_cmd([sys.executable, str(REPORT_B2)])


if __name__ == "__main__":
    main()
