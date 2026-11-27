"""Checkpoint 1 exact engine used by the streaming runner."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .contracts import StatsTuple
from .exact_agg import ExactAggregator
from .spark_state import SparkState


class ExactEngine:
    def __init__(self, spark: SparkSession, state_dir: str | None = None) -> None:
        self.state = SparkState(spark, state_dir=state_dir)
        self.aggregator = ExactAggregator()
        self._last_batch_id: int | None = None
        self._failed = False

    def _check_state(self) -> None:
        if self._failed:
            raise RuntimeError("A batch failed. Replay the stream with a fresh ExactEngine.")

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        self._check_state()
        # The runner assigns increasing IDs across epochs, so an earlier ID is a replay.
        if self._last_batch_id is not None and batch_id <= self._last_batch_id:
            return

        try:
            # Each slice covers the subjects affected by this batch, including subjects
            # that point to an object whose type changed. Spark can read both slices
            # lazily during update(), so their backing files must survive that call.
            before, after = self.state.apply_batch(batch_df)
            # Subtract the subjects' old contributions and add their new ones.
            self.aggregator.update(before, after)
            self.state.cleanup()
        except Exception:
            # A failure can leave the graph ahead of its counts. Block further reads
            # and updates on this instance because replay assumes they are in sync.
            self._failed = True
            raise
        self._last_batch_id = batch_id

    def on_epoch_end(self, epoch: int) -> None:
        """Statistics are updated after each successful batch."""

    def current_stats(self) -> StatsTuple:
        self._check_state()
        return self.aggregator.current_stats()

    def state_mb(self) -> float:
        """Size of the active Parquet buckets and the driver's summary dictionaries."""
        self._check_state()
        return (self.state.state_bytes() + self.aggregator.state_bytes()) / (1024 ** 2)
