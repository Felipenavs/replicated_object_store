import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

CONFIGS = {
    "single-node": BASE_DIR / "results_b2_single",
    "two-node": BASE_DIR / "results_b2_two_node",
    "three-node": BASE_DIR / "results_b2_three_node",
}


def load_summary(results_dir: Path):
    path = results_dir / "put_8" / "summary.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing: {path}")
    with open(path) as f:
        return json.load(f)


def fmt(val):
    return f"{val:.2f}" if val is not None else "N/A"


def main():
    rows = []

    for name, path in CONFIGS.items():
        summary = load_summary(path)

        rows.append({
            "configuration": name,
            "throughput": summary["throughput_ops_sec"],
            "p99": summary["p99_ms"],
        })

    md_path = BASE_DIR / "benchmark2_results.md"

    with open(md_path, "w") as f:
        f.write("| Configuration | Throughput (ops/sec) | p99 latency (ms) |\n")
        f.write("|---|---:|---:|\n")

        for row in rows:
            f.write(
                f"| {row['configuration']} | "
                f"{fmt(row['throughput'])} | "
                f"{fmt(row['p99'])} |\n"
            )

    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()