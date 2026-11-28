"""Automation entry point for Checkpoint 1 exact and Checkpoint 2 A1 grids."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
import tempfile
from itertools import product
from pathlib import Path


HLL_VALUES = (10, 12, 14)
CM_WIDTH_VALUES = (1 << 10, 1 << 12, 1 << 14)
DATASETS = ("toy", "dblp")
DELETION_STRATEGIES = {
    "s1": "per-epoch HLL merge",
    "s2": "Count-Min over entity hashes",
}


def parameter_grid(
    hll_values=HLL_VALUES,
    cm_width_values=CM_WIDTH_VALUES,
    deletion_strategies=tuple(DELETION_STRATEGIES),
):
    """Yield the exact baseline and the full A1 deletion/HLL/CM grid."""
    yield ("exact", None, {}, {})
    for strategy, p, width in product(
        deletion_strategies, hll_values, cm_width_values
    ):
        yield ("A1", strategy, {"hll_p": p}, {"cm_width": width})


def machine_specs() -> dict:
    """Collect machine and Python fields for experiment manifests."""
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "processor": platform.processor() or "unknown",
    }


def append_run_metadata(results_csv: str, run_metadata: dict) -> None:
    """Append a machine/configuration manifest alongside evaluation rows."""
    path = Path(results_csv)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest = path.with_suffix(".manifest.json")
    current = []
    if manifest.exists():
        current = json.loads(manifest.read_text(encoding="utf-8"))
    current.append(run_metadata)
    manifest.write_text(json.dumps(current, indent=2) + "\n", encoding="utf-8")


def config_variant(variant: str, strategy: str | None, hll: dict, cm: dict) -> str:
    """Return the variant label recorded in evaluation results."""
    if variant == "exact":
        return "exact"
    return f"{variant}-{strategy}-p{hll['hll_p']}-w{cm['cm_width']}"


def prediction_run_dir(
    pred_root: Path,
    dataset: str,
    variant_name: str,
    run: int,
) -> Path:
    """Return the prediction directory for one dataset/configuration/run."""
    return pred_root / dataset / variant_name / f"run_{run}"


def run_evaluation(
    data_dir: str,
    pred_dir: str,
    results_csv: str,
    epochs: list[int] | None = None,
    kit_dir: str | None = None,
    variant_name: str | None = None,
) -> None:
    """Run evaluate.py for existing gold/prediction files."""
    data = Path(data_dir)
    pred = Path(pred_dir)
    gold_dir = data / "gold"
    gold_files = sorted(
        gold_dir.glob("epoch*.json"),
        key=lambda p: int(p.stem.removeprefix("epoch")),
    )
    if epochs is not None:
        gold_files = [
            p for p in gold_files
            if int(p.stem.removeprefix("epoch")) in epochs
        ]
    if not gold_files:
        raise FileNotFoundError(f"No gold files found in {gold_dir}")

    with tempfile.TemporaryDirectory(prefix="shapestream_eval_") as eval_dir:
        for gold in gold_files:
            epoch = int(gold.stem.removeprefix("epoch"))
            predicted = pred / f"epoch{epoch}.json"
            if not predicted.exists():
                raise FileNotFoundError(
                    f"Prediction file missing: {predicted}. "
                    "The exact engine or sketch engine must generate it first."
                )
            eval_prediction = predicted
            if variant_name is not None:
                prediction_doc = json.loads(predicted.read_text(encoding="utf-8"))
                metadata = prediction_doc.get("meta")
                if not isinstance(metadata, dict):
                    raise ValueError(f"Prediction metadata must be an object: {predicted}")
                metadata["variant"] = variant_name
                eval_prediction = Path(eval_dir) / predicted.name
                eval_prediction.write_text(
                    json.dumps(prediction_doc, indent=1) + "\n",
                    encoding="utf-8",
                )

            run_info = pred / "run_info.json"
            cmd = [
                sys.executable, "-m", "shapestream.evaluate",
                "--gold", str(gold),
                "--pred", str(eval_prediction),
                "--results", str(results_csv),
            ]
            if run_info.exists():
                cmd.extend(["--run-info", str(run_info)])
            cwd = str(Path(kit_dir).resolve()) if kit_dir else None
            subprocess.run(cmd, check=True, cwd=cwd)


def main() -> None:
    """Parse experiment settings and execute or preview the configured grid."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--kit-dir", default="kit")
    ap.add_argument("--datasets", nargs="+", choices=DATASETS, default=["toy"])
    ap.add_argument("--epochs", nargs="+", type=int)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument(
        "--pred-root",
        help="Root containing <dataset>/<variant_config>/run_<n>/epoch<N>.json predictions",
    )
    ap.add_argument("--results", default="kit/results/results_A.csv")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    kit = Path(args.kit_dir).resolve()
    results = Path(args.results)
    if not results.is_absolute():
        results = kit.parent / results

    if args.dry_run:
        print(json.dumps({
            "runs_per_config": args.runs,
            "datasets": args.datasets,
            "grid": [
                {
                    "variant": config_variant(variant, strategy, hll, cm),
                    "strategy": strategy,
                    "deletion_strategy": DELETION_STRATEGIES.get(strategy),
                    "hll": hll,
                    "cm": cm,
                }
                for variant, strategy, hll, cm in parameter_grid()
            ],
            "machine": machine_specs(),
        }, indent=2))
        return

    if not args.pred_root:
        raise SystemExit("--pred-root is required unless --dry-run is used")

    pred_root = Path(args.pred_root)
    for dataset in args.datasets:
        data_dir = kit / "data" / dataset
        for variant, strategy, hll, cm in parameter_grid():
            for run in range(1, args.runs + 1):
                variant_name = config_variant(variant, strategy, hll, cm)
                pred_dir = prediction_run_dir(
                    pred_root, dataset, variant_name, run
                )
                metadata = {
                    "dataset": dataset,
                    "variant": variant_name,
                    "strategy": strategy,
                    "deletion_strategy": DELETION_STRATEGIES.get(strategy),
                    "run": run,
                    "hll_p": hll.get("hll_p"),
                    "cm_width": cm.get("cm_width"),
                    "spark_shuffle_partitions": 8,
                    **machine_specs(),
                }
                run_evaluation(
                    str(data_dir), str(pred_dir), str(results),
                    args.epochs, str(kit), variant_name,
                )
                append_run_metadata(str(results), metadata)


if __name__ == "__main__":
    main()
