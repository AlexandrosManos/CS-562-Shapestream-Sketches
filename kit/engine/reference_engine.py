"""Reference engine for testing streaming_runner.py (Member 1). NOT incremental.

It keeps the whole current triple set in the driver and recomputes the
statistics from scratch with the kit's batch extractor
(shapestream.gold_shapes.batch_stats) every time they are asked for. So its
output is exactly the gold whenever the runner fed it the right events in the
right order, which makes it a good oracle for the runner's own tests.

Use it only on the mock and toy data: it collects every event to the driver.
The team's real engines are ExactEngine (exact_engine.py) and SketchEngine
(sketch_engine.py).
"""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from shapestream.gold_shapes import batch_stats
from shapestream.spark_io import TRIPLE_SCHEMA

TRIPLE_COLS = ["s", "p", "o", "okind", "dt", "lang"]


class ReferenceEngine:
    """BaseEngine implementation that recomputes everything (tests only)."""

    def __init__(self, spark: SparkSession, **_params: Any) -> None:
        self.spark = spark
        self.triples: set[tuple] = set()

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.select("seq", "op", *TRIPLE_COLS).collect()
        for r in sorted(rows, key=lambda r: r["seq"]):
            t = tuple(r[c] for c in TRIPLE_COLS)
            if r["op"] == "+":
                self.triples.add(t)
            else:
                self.triples.discard(t)

    def on_epoch_end(self, epoch: int) -> None:
        pass

    def current_stats(self):
        df = self.spark.createDataFrame(list(self.triples), schema=TRIPLE_SCHEMA)
        return batch_stats(df)

    def state_mb(self) -> float:
        n_bytes = sum(len(x) for t in self.triples for x in t if x)
        return n_bytes / (1 << 20)
