import json
from pathlib import Path
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "results"
PLOTS_DIR = BASE_DIR / "plots"

CONCURRENCY = [1, 2, 4, 8, 16, 32]


def load_summary(op, clients):
    path = RESULTS_DIR / f"{op}_{clients}" / "summary.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing summary file: {path}")
    with open(path) as f:
        return json.load(f)


def plot_benchmark1():
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

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    plot_benchmark1()