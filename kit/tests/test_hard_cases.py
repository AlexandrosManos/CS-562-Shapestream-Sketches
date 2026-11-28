"""Checkpoint 1 hard-case tests for the ShapeStream exact engine."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path


KIT_DIR = Path(__file__).resolve().parents[1]
CONTRACT_DIR = KIT_DIR / "examples_contract"
if str(KIT_DIR) not in sys.path:
    sys.path.insert(0, str(KIT_DIR))

from engine.exact_agg import ExactAggregator
from engine.spark_state import SparkState
from shapestream.common import read_stats, write_stats
from shapestream.evaluate import evaluate
from shapestream.spark_io import EVENT_SCHEMA, get_spark


class HardCaseTests(unittest.TestCase):
    def _assert_stream_matches_gold(self, dataset: str):
        spark = get_spark(f"exact_{dataset}_tests")
        state = SparkState(spark)
        aggregator = ExactAggregator()
        stream_dir = KIT_DIR / "data" / dataset / "streams" / "full"
        gold_dir = KIT_DIR / "data" / dataset / "gold"

        try:
            with tempfile.TemporaryDirectory(prefix=f"shapestream_{dataset}_pred_") as pred_dir:
                for epoch, gold_path in enumerate(sorted(gold_dir.glob("epoch*.json"))):
                    batch = spark.read.schema(EVENT_SCHEMA).json(
                        str(stream_dir / f"epoch={epoch}")
                    )
                    before_slice, after_slice = state.apply_batch(batch)
                    aggregator.update(before_slice, after_slice)
                    state.cleanup()

                    pred_path = Path(pred_dir) / gold_path.name
                    write_stats(
                        str(pred_path),
                        {
                            "team": "A1",
                            "variant": "exact",
                            "dataset": dataset,
                            "epoch": epoch,
                        },
                        *aggregator.current_stats(),
                    )

                    _, gold_classes, gold_properties, gold_constraints = read_stats(
                        str(gold_path)
                    )
                    _, pred_classes, pred_properties, pred_constraints = read_stats(
                        str(pred_path)
                    )
                    with self.subTest(dataset=dataset, epoch=epoch):
                        self.assertEqual(pred_classes, gold_classes)
                        self.assertEqual(pred_properties, gold_properties)
                        self.assertEqual(pred_constraints, gold_constraints)

                        _, rows, extra = evaluate(str(gold_path), str(pred_path))
                        for row in rows:
                            self.assertEqual(row["f1"], 1.0)
                            self.assertEqual(row["support_mape"], 0.0)
                        self.assertEqual(extra["cardinality_accuracy"], 1.0)
        finally:
            spark.stop()

    def test_stream_epochs_match_expected_stats(self):
        """Run all hard-case stream batches through state and exact aggregation."""
        spark = get_spark("hard_case_tests")
        state = SparkState(spark)
        aggregator = ExactAggregator()
        try:
            for epoch in range(3):
                events_path = CONTRACT_DIR / f"1_stream_events_epoch{epoch}.jsonl"
                batch = spark.read.schema(EVENT_SCHEMA).json(str(events_path))
                before_slice, after_slice = state.apply_batch(batch)
                aggregator.update(before_slice, after_slice)
                state.cleanup()

                expected_path = CONTRACT_DIR / f"3_expected_stats_epoch{epoch}.json"
                _, expected_classes, expected_properties, expected_constraints = (
                    read_stats(str(expected_path))
                )
                self.assertEqual(
                    aggregator.current_stats(),
                    (expected_classes, expected_properties, expected_constraints),
                    f"stats mismatch after epoch {epoch}",
                )
        finally:
            spark.stop()

    def test_toy_gold_reaches_perfect_accuracy(self):
        """Check that exact stream results equal every toy gold snapshot."""
        gold = KIT_DIR / "data/toy/gold"
        if not gold.exists():
            self.skipTest("toy gold snapshots are not present")
        self._assert_stream_matches_gold("toy")

    @unittest.skipUnless(
        os.environ.get("SHAPESTREAM_RUN_DBLP_TESTS") == "1",
        "set SHAPESTREAM_RUN_DBLP_TESTS=1 to run the slower DBLP exact-stream test",
    )
    def test_dblp_gold_reaches_perfect_accuracy_when_available(self):
        """Check that exact stream results equal every DBLP gold snapshot."""
        gold = KIT_DIR / "data/dblp/gold"
        if not gold.exists():
            self.skipTest("DBLP gold snapshots are not present")
        self._assert_stream_matches_gold("dblp")


if __name__ == "__main__":
    unittest.main()
