import csv
import json
from pathlib import Path

import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "results"
PLOTS_DIR = BASE_DIR / "plots"
REPORTS_DIR = BASE_DIR / "reports"
CONCURRENCY = [1, 2, 4, 8, 16, 32]


def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with open(path) as f:
        return json.load(f)


def load_summary(op: str, clients: int):
    path = RESULTS_DIR / f"{op}_{clients}" / "summary.json"
    return load_json(path)


def write_plot_data(put_tp, get_tp):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = REPORTS_DIR / "benchmark1_plot_data.csv"
    with open(out_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["clients", "put_throughput_ops_sec", "get_throughput_ops_sec"])
        for clients, put_val, get_val in zip(CONCURRENCY, put_tp, get_tp):
            writer.writerow([clients, put_val, get_val])
    print(out_csv)


def main():
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    put_tp = [load_summary("put", c)["throughput_ops_sec"] for c in CONCURRENCY]
    get_tp = [load_summary("get", c)["throughput_ops_sec"] for c in CONCURRENCY]

    plt.figure(figsize=(8, 5))
    plt.plot(CONCURRENCY, put_tp, marker="o", label="put")
    plt.plot(CONCURRENCY, get_tp, marker="o", label="get")
    plt.xscale("log", base=2)
    plt.xticks(CONCURRENCY, CONCURRENCY)
    plt.xlabel("Concurrent client processes")
    plt.ylabel("Throughput (ops/sec)")
    plt.title("Benchmark 1: Throughput vs Concurrency")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    out_path = PLOTS_DIR / "benchmark1_throughput.png"
    plt.savefig(out_path, dpi=200)
    plt.close()

    write_plot_data(put_tp, get_tp)
    print(out_path)


if __name__ == "__main__":
    main()
