"""Time exact batch re-extraction as the non-incremental baseline."""
from __future__ import annotations
import argparse
import json
import sys
import time
from pathlib import Path
from typing import Iterable


KIT_DIR = Path(__file__).resolve().parents[1]
if str(KIT_DIR) not in sys.path:
    sys.path.insert(0, str(KIT_DIR))


def measure_batch_reextraction(
    data_dir: str,
    epochs: Iterable[int] | None = None,
    repeats: int = 3,
    driver_memory: str | None = None,
) -> dict[int, dict[str, float]]:
    """Measure gold_shapes.batch_stats() once per epoch and per repeat."""
    try:
        from shapestream.gold_shapes import batch_stats
        from shapestream.spark_io import get_spark
    except ImportError as exc:
        raise RuntimeError("Run this script from kit/ with the kit package available") from exc

    data = Path(data_dir)
    snapshots = data / "snapshots"
    available = sorted(
        int(p.name.rsplit("=", 1)[1])
        for p in snapshots.glob("snap=*")
        if p.is_dir()
    )
    selected = available if epochs is None else [k for k in epochs if k in available]
    if not selected:
        raise ValueError(f"No requested snapshot epochs found under {snapshots}")

    spark = get_spark("benchmark_batch", driver_memory)
    timings: dict[int, dict[str, float]] = {}
    try:
        for epoch in selected:
            values = []
            for _ in range(repeats):
                t = spark.read.parquet(str(snapshots / f"snap={epoch}"))
                start = time.perf_counter()
                batch_stats(t)
                values.append(time.perf_counter() - start)
            timings[epoch] = {
                "mean_s": sum(values) / len(values),
                "min_s": min(values),
                "max_s": max(values),
            }
    finally:
        spark.stop()
    return timings


def main() -> None:
    """Parse benchmark arguments, run measurements, and optionally write JSON."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Dataset directory, for example data/toy")
    ap.add_argument("--epochs", nargs="+", type=int, help="Snapshot epochs to measure")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--driver-memory")
    ap.add_argument("--output", help="Optional JSON output path")
    args = ap.parse_args()

    timings = measure_batch_reextraction(
        args.data, args.epochs, args.repeats, args.driver_memory
    )
    output = json.dumps({str(k): v for k, v in timings.items()}, indent=2)
    print(output)
    if args.output:
        Path(args.output).write_text(output + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
