"""Generate the four publication plots required for Checkpoint 2/report."""
from __future__ import annotations
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


KIT_DIR = Path(__file__).resolve().parents[1]


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


def _tradeoff_averages(rows: list[dict]) -> dict[int, dict[str, tuple[float, float]]]:
    """Return mean (state_mb, support_mape) per epoch and variant at sigma=10, tau=0.1."""
    samples = defaultdict(lambda: {"state": [], "mape": []})
    for row in rows:
        if _float(row, "sigma") != 10 or _float(row, "tau") != 0.1:
            continue
        state = _float(row, "state_mb")
        mape = _float(row, "support_mape")
        if state is None or mape is None:
            continue
        variant = row.get("variant", "unknown")
        sample = samples[(int(row["epoch"]), variant)]
        sample["state"].append(state)
        sample["mape"].append(mape)

    averages = defaultdict(dict)
    for (epoch, variant), sample in samples.items():
        averages[epoch][variant] = (
            sum(sample["state"]) / len(sample["state"]),
            sum(sample["mape"]) / len(sample["mape"]),
        )
    return dict(averages)


def _deletion_growth_averages(rows: list[dict]) -> dict[tuple[str, str], dict[int, float]]:
    """Return mean support MAPE per dataset, strategy, and epoch."""
    samples = defaultdict(list)
    for row in rows:
        if _float(row, "sigma") != 10 or _float(row, "tau") != 0.1:
            continue
        variant = row.get("variant", "")
        parts = variant.split("-")
        if len(parts) < 4 or parts[0] != "A1" or not parts[1].startswith("s"):
            continue
        mape = _float(row, "support_mape")
        dataset = row.get("dataset")
        if mape is None or not dataset:
            continue
        samples[(dataset, parts[1], int(row["epoch"]))].append(mape)

    averages = defaultdict(dict)
    for (dataset, strategy, epoch), mapes in samples.items():
        averages[(dataset, strategy)][epoch] = sum(mapes) / len(mapes)
    return dict(averages)


def _cumulative_deletions(manifest_path: Path) -> dict[int, int]:
    """Read per-epoch deletion totals and return cumulative totals by epoch."""
    with manifest_path.open(encoding="utf-8") as f:
        manifest = json.load(f)
    cumulative = 0
    totals = {}
    for epoch_info in sorted(manifest["epochs"], key=lambda item: int(item["epoch"])):
        epoch = int(epoch_info["epoch"])
        cumulative += int(epoch_info["deletions"])
        totals[epoch] = cumulative
    return totals


def generate_plots(results_csv: str, output_dir: str) -> list[Path]:
    """Generate the F1, accuracy-memory, latency, and deletion-growth plots."""
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

    f1_by_variant_epoch = defaultdict(lambda: defaultdict(list))
    for row in rows:
        if _float(row, "sigma") == 10 and _float(row, "tau") == 0.1:
            variant = row.get("variant", "unknown")
            f1_by_variant_epoch[variant][int(row["epoch"])].append(_float(row, "f1"))
    fig, ax = plt.subplots(figsize=(6, 4))
    for variant, by_epoch in sorted(f1_by_variant_epoch.items()):
        epochs = sorted(by_epoch)
        means = [sum(by_epoch[e]) / len(by_epoch[e]) for e in epochs]
        ax.plot(epochs, means, marker="o", label=variant)
    ax.set(xlabel="Epoch", ylabel="F1", title=r"F1 at $\sigma=10,\ \tau=0.1$")
    ax.grid(True, alpha=0.3)
    if ax.lines:
        ax.legend()
    fig.tight_layout()
    f1_path = out / "f1_per_epoch.png"
    fig.savefig(f1_path, dpi=300)
    plt.close(fig)

    tradeoff_by_epoch = _tradeoff_averages(rows)
    if not tradeoff_by_epoch:
        raise ValueError("No tradeoff rows found at sigma=10, tau=0.1")
    tradeoff_epochs = sorted(tradeoff_by_epoch)
    fig, axes = plt.subplots(
        1,
        len(tradeoff_epochs),
        figsize=(6 * len(tradeoff_epochs), 4),
        squeeze=False,
        sharey=True,
    )
    for epoch, ax in zip(tradeoff_epochs, axes[0]):
        points_by_strategy = defaultdict(list)
        exact_point = None
        for variant, point in tradeoff_by_epoch[epoch].items():
            if variant == "exact":
                exact_point = point
                continue
            parts = variant.split("-")
            strategy = parts[1] if len(parts) > 1 and parts[0] == "A1" else variant
            points_by_strategy[strategy].append(point)

        for strategy, points in sorted(points_by_strategy.items()):
            points.sort(key=lambda point: point[0])
            ax.plot(
                [point[0] for point in points],
                [point[1] for point in points],
                marker="o",
                label=strategy,
            )
        if exact_point is not None:
            ax.scatter(*exact_point, marker="*", s=100, label="exact")
        ax.set(
            xlabel="State memory (MB)",
            ylabel="Support MAPE",
            title=f"Epoch {epoch}",
        )
        ax.grid(True, alpha=0.3)
        if ax.lines or ax.collections:
            ax.legend()
    fig.tight_layout()
    tradeoff_path = out / "support_mape_vs_state_mb.png"
    fig.savefig(tradeoff_path, dpi=300)
    plt.close(fig)

    growth_by_dataset_strategy = _deletion_growth_averages(rows)
    if not growth_by_dataset_strategy:
        raise ValueError("No A1 support MAPE rows found at sigma=10, tau=0.1")
    datasets = sorted({dataset for dataset, _ in growth_by_dataset_strategy})
    fig, axes = plt.subplots(
        1,
        len(datasets),
        figsize=(6 * len(datasets), 4),
        squeeze=False,
        sharey=True,
    )
    for dataset, ax in zip(datasets, axes[0]):
        manifest_path = KIT_DIR / "data" / dataset / "streams" / "full" / "manifest.json"
        deletion_counts = _cumulative_deletions(manifest_path)
        for (series_dataset, strategy), by_epoch in sorted(
            growth_by_dataset_strategy.items()
        ):
            if series_dataset != dataset:
                continue
            missing_epochs = set(by_epoch) - deletion_counts.keys()
            if missing_epochs:
                raise ValueError(
                    f"Stream manifest has no deletion totals for {dataset} "
                    f"epochs {sorted(missing_epochs)}"
                )
            points = sorted(
                (deletion_counts[epoch], mape)
                for epoch, mape in by_epoch.items()
            )
            ax.plot(
                [deletions for deletions, _ in points],
                [mape for _, mape in points],
                marker="o",
                label=strategy,
            )
        ax.set(
            xlabel="Cumulative deletions",
            ylabel="Support MAPE",
            title=dataset,
        )
        ax.grid(True, alpha=0.3)
        if ax.lines:
            ax.legend(title="Deletion strategy")
    fig.tight_layout()
    deletion_growth_path = out / "support_mape_vs_cumulative_deletions.png"
    fig.savefig(deletion_growth_path, dpi=300)
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

    return [f1_path, tradeoff_path, deletion_growth_path, latency_path]


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
