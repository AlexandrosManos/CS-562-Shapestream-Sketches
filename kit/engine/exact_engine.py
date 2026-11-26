"""Checkpoint 1 exact engine used by the streaming runner."""
from __future__ import annotations

from tempfile import TemporaryDirectory
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession

from .contracts import StatsTuple
from .exact_agg import ExactAggregator
from .spark_state import SparkState


class ExactEngine:
    def __init__(self, spark: SparkSession, state_dir: str | None = None) -> None:
        # SparkState writes immutable batch versions. Give each run its own path.
        self._temp_dir: TemporaryDirectory | None = None
        if state_dir is None:
            self._temp_dir = TemporaryDirectory()
            state_dir = self._temp_dir.name
        self._run_dir = f"{state_dir.rstrip('/')}/run_{uuid4().hex}"
        self.state = SparkState(spark, state_dir=self._run_dir)
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
        """Size of the current Parquet version and the driver's summary dictionaries."""
        self._check_state()
        table_bytes = 0
        # SparkState increments state_version after writing a batch. Zero means
        # nothing has been written yet, and version N makes batch_(N-1) active.
        if self.state.state_version:
            context = self.state.session.sparkContext
            path = context._jvm.org.apache.hadoop.fs.Path(
                f"{self._run_dir}/batch_{self.state.state_version - 1}"
            )
            filesystem = path.getFileSystem(context._jsc.hadoopConfiguration())
            table_bytes = filesystem.getContentSummary(path).getLength()
        return (table_bytes + self.aggregator.state_bytes()) / (1024 ** 2)
