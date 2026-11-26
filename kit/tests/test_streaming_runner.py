"""Tests for engine/streaming_runner.py (Member 1).

Run from the kit folder inside the course container:

    python -m unittest tests.test_streaming_runner -v
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

KIT_DIR = Path(__file__).resolve().parents[1]
if str(KIT_DIR) not in sys.path:
    sys.path.insert(0, str(KIT_DIR))

from engine.reference_engine import ReferenceEngine  # noqa: E402
from engine.streaming_runner import (  # noqa: E402
    StreamingRunner, config_name, dataset_name, epoch_dirs, make_engine,
)
from shapestream.common import read_stats  # noqa: E402
from shapestream.evaluate import evaluate  # noqa: E402
from shapestream.spark_io import get_spark  # noqa: E402

CONTRACT_DIR = KIT_DIR / "examples_contract"
TOY_STREAM = KIT_DIR / "data" / "toy" / "streams" / "full"
TOY_GOLD = KIT_DIR / "data" / "toy" / "gold"
META = {"team": "A1", "variant": "test", "dataset": "mock"}


def write_mock_stream(dst: Path, split_epoch0_into: int = 1) -> Path:
    """Copy examples_contract/1_stream_events_epoch<k>.jsonl into dst/epoch=<k>/.

    With split_epoch0_into > 1, epoch 0 is spread over that many files, so that
    maxFilesPerTrigger=1 gives several micro-batches in one epoch.
    """
    for k in range(3):
        lines = (CONTRACT_DIR / f"1_stream_events_epoch{k}.jsonl").read_text().splitlines()
        ep = dst / f"epoch={k}"
        ep.mkdir(parents=True)
        parts = split_epoch0_into if k == 0 else 1
        size = -(-len(lines) // parts)
        for i in range(parts):
            chunk = lines[i * size:(i + 1) * size]
            if chunk:
                (ep / f"part-{i:05d}.json").write_text("\n".join(chunk) + "\n")
    return dst


class RecordingEngine:
    """Records the calls the runner makes, returns empty statistics."""

    def __init__(self, spark=None, **_params):
        self.calls = []

    def process_batch(self, batch_df, batch_id):
        rows = batch_df.collect()
        self.calls.append(("batch", batch_id, {r["epoch"] for r in rows},
                           {r["op"] for r in rows}, len(rows), batch_df.columns))

    def on_epoch_end(self, epoch):
        self.calls.append(("end", epoch))

    def current_stats(self):
        self.calls.append(("stats",))
        return {}, {}, {}

    def state_mb(self):
        return None


class StreamingRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("test_streaming_runner")
        cls.tmp = Path(tempfile.mkdtemp(prefix="runner_test_"))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _dir(self, name: str) -> Path:
        d = self.tmp / f"{self.id().rsplit('.', 1)[-1]}_{name}"
        shutil.rmtree(d, ignore_errors=True)
        return d

    def _run(self, engine, stream: Path, out: Path, **kw) -> dict:
        runner = StreamingRunner(self.spark, engine, str(out), META, **kw)
        return runner.run_files(str(stream))

    def assert_same_stats(self, expected_path, pred_path):
        _, ec, ep, ek = read_stats(str(expected_path))
        _, pc, pp, pk = read_stats(str(pred_path))
        self.assertEqual(ec, pc, "classes differ")
        self.assertEqual(ep, pp, "properties differ")
        self.assertEqual(ek, pk, "constraints differ")

    # ------------------------------------------------------------------ #
    def test_mock_contract_gives_expected_stats(self):
        """Mock events -> 3_expected_stats_epoch{0,1,2}.json, plus run_info per epoch."""
        stream = write_mock_stream(self._dir("stream"))
        out = self._dir("out")
        info = self._run(ReferenceEngine(self.spark), stream, out)

        for k in range(3):
            self.assert_same_stats(CONTRACT_DIR / f"3_expected_stats_epoch{k}.json", out / f"epoch{k}.json")
            meta, *_ = read_stats(str(out / f"epoch{k}.json"))
            self.assertEqual(meta, {**META, "epoch": k})

        on_disk = json.loads((out / "run_info.json").read_text())
        self.assertEqual(on_disk, info)
        self.assertEqual(sorted(on_disk), ["0", "1", "2"])
        self.assertEqual([on_disk[str(k)]["events"] for k in range(3)], [8, 3, 1])
        for k in range(3):
            for field in ("latency_s", "events_per_s", "state_mb"):
                self.assertIsNotNone(on_disk[str(k)][field], field)

    def test_several_micro_batches_per_epoch(self):
        """Epoch 0 split over 4 files with maxFilesPerTrigger=1 still gives the expected stats."""
        stream = write_mock_stream(self._dir("stream"), split_epoch0_into=4)
        out = self._dir("out")
        rec = RecordingEngine()
        self._run(rec, stream, out, max_files_per_trigger=1)
        epoch0_batches = [c for c in rec.calls if c[0] == "batch" and c[2] == {0}]
        self.assertEqual(len(epoch0_batches), 4)

        out2 = self._dir("out2")
        self._run(ReferenceEngine(self.spark), stream, out2, max_files_per_trigger=1)
        for k in range(3):
            self.assert_same_stats(CONTRACT_DIR / f"3_expected_stats_epoch{k}.json", out2 / f"epoch{k}.json")

    def test_call_order_and_batch_contents(self):
        """Per epoch: process_batch* (only that epoch, ops +/-) -> on_epoch_end(k) -> current_stats."""
        stream = write_mock_stream(self._dir("stream"))
        rec = RecordingEngine()
        self._run(rec, stream, self._dir("out"))

        expected_kinds = []
        epoch = 0
        batch_ids = []
        for call in rec.calls:
            if call[0] == "batch":
                _, bid, epochs, ops, n, cols = call
                self.assertEqual(epochs, {epoch}, "a micro-batch mixed epochs")
                self.assertTrue(ops <= {"+", "-"})
                self.assertGreater(n, 0)
                self.assertIn("seq", cols)
                batch_ids.append(bid)
            elif call[0] == "end":
                self.assertEqual(call[1], epoch)
            else:
                epoch += 1
            expected_kinds.append(call[0])
        self.assertEqual(expected_kinds, ["batch", "end", "stats"] * 3)
        self.assertEqual(batch_ids, sorted(set(batch_ids)), "batch ids must grow across epochs")

    def test_engine_without_on_epoch_end(self):
        """on_epoch_end is optional."""
        class NoHook(RecordingEngine):
            on_epoch_end = None
        rec = NoHook()
        self._run(rec, write_mock_stream(self._dir("stream")), self._dir("out"))
        self.assertEqual([c[0] for c in rec.calls], ["batch", "stats"] * 3)

    def test_rerun_into_same_out_dir_reprocesses_everything(self):
        """A second run must not skip the files because of old streaming checkpoints."""
        stream = write_mock_stream(self._dir("stream"))
        out = self._dir("out")
        self._run(RecordingEngine(), stream, out)
        info = self._run(RecordingEngine(), stream, out)
        self.assertEqual([info[str(k)]["events"] for k in range(3)], [8, 3, 1])

    @unittest.skipUnless(TOY_STREAM.exists() and TOY_GOLD.exists(), "toy data not present")
    def test_toy_stream_matches_gold(self):
        """Full toy stream through the runner: F1 = 1.0, MAPE = 0 at every threshold."""
        out = self._dir("out")
        self._run(ReferenceEngine(self.spark), TOY_STREAM, out)
        for k, _ in epoch_dirs(str(TOY_STREAM)):
            _, rows, extra = evaluate(str(TOY_GOLD / f"epoch{k}.json"), str(out / f"epoch{k}.json"))
            for row in rows:
                self.assertEqual(row["f1"], 1.0, f"epoch {k} {row}")
                self.assertEqual(row["support_mape"], 0.0, f"epoch {k} {row}")
            self.assertEqual(extra["cardinality_accuracy"], 1.0)
            self.assertEqual(extra["class_count_mape"], 0.0)

    # ------------------------------------------------------------------ #
    def test_helpers(self):
        self.assertEqual(config_name("exact"), "exact")
        self.assertEqual(config_name("sketch", 12, 4096), "sketch_p12_w4096")
        self.assertEqual(config_name("sketch", 10, 1024, "epochmerge"), "sketch_p10_w1024_epochmerge")
        self.assertEqual(dataset_name("data/toy/streams/full"), "toy")
        self.assertEqual(dataset_name("data/dblp/streams/full/"), "dblp")
        self.assertIsInstance(make_engine("reference", self.spark, state_dir="x"), ReferenceEngine)
        if not (KIT_DIR / "engine" / "exact_engine.py").exists():
            with self.assertRaises(SystemExit):
                make_engine("exact", self.spark, state_dir="x")


if __name__ == "__main__":
    unittest.main()
