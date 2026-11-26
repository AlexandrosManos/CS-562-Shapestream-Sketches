"""Generate the three publication plots required for Checkpoint 2/report."""
from __future__ import annotations
import argparse
import csv
from collections import defaultdict
from pathlib import Path


def _rows(path: str) -> list[dict]:
    """Read a results CSV into a list of dictionaries."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _float(row: dict, key: str):
    """Return an optional numeric CSV field as a float."""
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def generate_plots(results_csv: str, output_dir: str) -> list[Path]:
    """Generate the F1, accuracy-memory, and latency publication plots."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required to generate plots") from exc

    rows = _rows(results_csv)
    if not rows:
        raise ValueError("Results CSV contains no rows")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    f1_by_epoch = defaultdict(list)
    for row in rows:
        if _float(row, "sigma") == 10 and _float(row, "tau") == 0.1:
            f1_by_epoch[int(row["epoch"])].append(_float(row, "f1"))
    fig, ax = plt.subplots(figsize=(6, 4))
    if f1_by_epoch:
        epochs = sorted(f1_by_epoch)
        means = [sum(f1_by_epoch[e]) / len(f1_by_epoch[e]) for e in epochs]
        ax.plot(epochs, means, marker="o")
    ax.set(xlabel="Epoch", ylabel="F1", title=r"F1 at $\sigma=10,\ \tau=0.1$")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    f1_path = out / "f1_per_epoch.png"
    fig.savefig(f1_path, dpi=300)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    groups = defaultdict(list)
    for row in rows:
        state = _float(row, "state_mb")
        mape = _float(row, "support_mape")
        if state is not None and mape is not None:
            groups[row.get("variant", "unknown")].append((state, mape))
    for label, points in sorted(groups.items()):
        points.sort()
        ax.plot([p[0] for p in points], [p[1] for p in points], marker="o", label=label)
    ax.set(xlabel="State memory (MB)", ylabel="Support MAPE")
    ax.grid(True, alpha=0.3)
    if ax.lines:
        ax.legend()
    fig.tight_layout()
    tradeoff_path = out / "support_mape_vs_state_mb.png"
    fig.savefig(tradeoff_path, dpi=300)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    latency_groups = defaultdict(list)
    for row in rows:
        latency = _float(row, "latency_s")
        if latency is not None:
            latency_groups[row.get("variant", "unknown")].append(
                (int(row["epoch"]), latency)
            )
    for label, points in sorted(latency_groups.items()):
        by_epoch = defaultdict(list)
        for epoch, latency in points:
            by_epoch[epoch].append(latency)
        epochs = sorted(by_epoch)
        means = [sum(by_epoch[e]) / len(by_epoch[e]) for e in epochs]
        ax.plot(epochs, means, marker="o", label=label)
    ax.set(xlabel="Epoch", ylabel="Latency (s)", title="Latency per epoch")
    ax.grid(True, alpha=0.3)
    if ax.lines:
        ax.legend()
    fig.tight_layout()
    latency_path = out / "latency_per_epoch.png"
    fig.savefig(latency_path, dpi=300)
    plt.close(fig)

    return [f1_path, tradeoff_path, latency_path]


def main() -> None:
    """Parse plot arguments and generate all configured figures."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="kit/results/results_A.csv")
    ap.add_argument("--output-dir", default="kit/results/figures")
    args = ap.parse_args()
    for path in generate_plots(args.results, args.output_dir):
        print(path)


if __name__ == "__main__":
    main()
