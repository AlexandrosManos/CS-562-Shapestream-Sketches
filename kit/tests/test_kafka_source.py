"""Tests for the Kafka source of engine/streaming_runner.py (Member 1).

Most tests hand-make the micro-batches that Kafka could deliver (epochs and
partitions mixed in every way) and pass them to on_kafka_batch, so they need
no Kafka. The last test replays the toy stream into a real topic and is
skipped when Kafka is not reachable.

Run from the kit folder inside the course container:

    python -m unittest tests.test_kafka_source -v
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
import zlib
from pathlib import Path

KIT_DIR = Path(__file__).resolve().parents[1]
if str(KIT_DIR) not in sys.path:
    sys.path.insert(0, str(KIT_DIR))

from pyspark.sql import types as T  # noqa: E402

import tests.test_streaming_runner as base  # noqa: E402
from engine.reference_engine import ReferenceEngine  # noqa: E402
from engine.streaming_runner import StreamingRunner  # noqa: E402
from shapestream.common import read_stats  # noqa: E402
from shapestream.spark_io import EVENT_SCHEMA, get_spark  # noqa: E402

PARTITIONS = {0, 1, 2}
KAFKA_BATCH_SCHEMA = T.StructType([T.StructField("partition", T.IntegerType())] + EVENT_SCHEMA.fields)
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def _row(partition: int, event: dict) -> tuple:
    return (partition, *(event.get(f) for f in EVENT_SCHEMA.fieldNames()))


def mock_partition_streams() -> dict[int, list[tuple]]:
    """The mock events per partition, in the order replay_to_kafka sends them:
    events spread over the partitions by subject, and after each epoch one
    checkpoint marker on every partition."""
    streams = {p: [] for p in PARTITIONS}
    for k in range(3):
        for line in (base.CONTRACT_DIR / f"1_stream_events_epoch{k}.jsonl").read_text().splitlines():
            event = json.loads(line)
            p = zlib.crc32(event["s"].encode()) % len(PARTITIONS)
            streams[p].append(_row(p, event))
        for p in PARTITIONS:
            streams[p].append(_row(p, {"op": "checkpoint", "epoch": k}))
    return streams


def one_batch(streams):
    """Everything in a single micro-batch (all epochs and markers mixed)."""
    return [[r for p in sorted(streams) for r in streams[p]]]


def one_partition_per_batch(streams):
    """Partition 0 delivers all its epochs before partition 1 starts, and so on."""
    return [streams[p] for p in sorted(streams)]


def round_robin(streams, n):
    """n rows of every partition per micro-batch."""
    longest = max(len(rows) for rows in streams.values())
    return [[r for p in sorted(streams) for r in streams[p][i:i + n]] for i in range(0, longest, n)]


class KafkaSplitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("test_kafka_source")
        cls.tmp = Path(tempfile.mkdtemp(prefix="kafka_test_"))
        cls.streams = mock_partition_streams()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _out(self, name: str) -> Path:
        d = self.tmp / f"{self.id().rsplit('.', 1)[-1]}_{name}"
        shutil.rmtree(d, ignore_errors=True)
        return d

    def _runner(self, engine, out: Path) -> StreamingRunner:
        runner = StreamingRunner(self.spark, engine, str(out), base.META)
        runner.reset_kafka(num_epochs=3, partitions=PARTITIONS)
        return runner

    def _send(self, runner: StreamingRunner, batches) -> None:
        for i, rows in enumerate(batches):
            runner.on_kafka_batch(self.spark.createDataFrame(rows, KAFKA_BATCH_SCHEMA), i)

    def assert_expected_stats(self, out: Path):
        for k in range(3):
            _, ec, ep, ek = read_stats(str(base.CONTRACT_DIR / f"3_expected_stats_epoch{k}.json"))
            _, pc, pp, pk = read_stats(str(out / f"epoch{k}.json"))
            self.assertEqual((ec, ep, ek), (pc, pp, pk), f"epoch {k}")

    def test_mock_partitions_are_mixed(self):
        """Sanity check of the fixture: the events use more than one partition."""
        with_events = [p for p, rows in self.streams.items() if any(r[3] != "checkpoint" for r in rows)]
        self.assertGreater(len(with_events), 1)

    def test_every_interleaving_gives_expected_stats(self):
        cases = {
            "one_batch": one_batch(self.streams),
            "one_partition_per_batch": one_partition_per_batch(self.streams),
            "round_robin_1": round_robin(self.streams, 1),
            "round_robin_3": round_robin(self.streams, 3),
        }
        for name, batches in cases.items():
            with self.subTest(name):
                out = self._out(name)
                runner = self._runner(ReferenceEngine(self.spark), out)
                self._send(runner, batches)
                self.assertTrue(runner.kafka_done)
                self.assert_expected_stats(out)
                info = json.loads((out / "run_info.json").read_text())
                self.assertEqual([info[str(k)]["events"] for k in range(3)], [8, 3, 1])
                self.assertFalse((out / "_pending").exists() and any((out / "_pending").iterdir()))

    def test_engine_never_sees_two_epochs_at_once(self):
        for name, batches in {"one_partition_per_batch": one_partition_per_batch(self.streams),
                              "round_robin_1": round_robin(self.streams, 1)}.items():
            with self.subTest(name):
                rec = base.RecordingEngine()
                self._send(self._runner(rec, self._out(name)), batches)
                epoch, kinds = 0, []
                for call in rec.calls:
                    if call[0] == "batch":
                        self.assertEqual(call[2], {epoch}, "a micro-batch mixed epochs")
                    elif call[0] == "end":
                        self.assertEqual(call[1], epoch)
                    else:
                        epoch += 1
                    if not kinds or kinds[-1] != call[0]:
                        kinds.append(call[0])
                self.assertEqual(kinds, ["batch", "end", "stats"] * 3)

    def test_epoch_waits_for_the_marker_of_every_partition(self):
        out = self._out("out")
        runner = self._runner(ReferenceEngine(self.spark), out)
        self._send(runner, [self.streams[0] + self.streams[1]])
        self.assertFalse((out / "epoch0.json").exists())
        self.assertEqual(runner.current_epoch, 0)
        self._send(runner, [self.streams[2]])
        self.assertTrue(runner.kafka_done)
        self.assert_expected_stats(out)

    def test_event_of_a_closed_epoch_is_an_error(self):
        runner = self._runner(base.RecordingEngine(), self._out("out"))
        epoch0 = [r for p in sorted(self.streams) for r in self.streams[p] if r[2] == 0]
        self._send(runner, [epoch0])
        self.assertEqual(runner.current_epoch, 1)
        late_event = next(r for r in epoch0 if r[3] != "checkpoint")
        with self.assertRaises(RuntimeError):
            self._send(runner, [[late_event]])


def _kafka_reachable() -> bool:
    try:
        from kafka import KafkaAdminClient
        KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=3000).close()
        return True
    except Exception:
        return False


@unittest.skipUnless(base.TOY_STREAM.exists() and base.TOY_GOLD.exists(), "toy data not present")
@unittest.skipUnless(_kafka_reachable(), f"Kafka not reachable at {BOOTSTRAP}")
class RealKafkaTests(unittest.TestCase):
    def test_toy_replay_matches_gold(self):
        """replay_to_kafka --rate 0 into a fresh topic, read it back in small micro-batches."""
        from kafka import KafkaAdminClient

        topic = f"test-runner-{uuid.uuid4().hex[:8]}"
        spark = get_spark("test_kafka_source")
        out = Path(tempfile.mkdtemp(prefix="kafka_real_"))
        try:
            subprocess.run([sys.executable, "-m", "shapestream.replay_to_kafka", "--stream", str(base.TOY_STREAM),
                            "--topic", topic, "--bootstrap", BOOTSTRAP, "--rate", "0"],
                           cwd=KIT_DIR, check=True, capture_output=True)
            runner = StreamingRunner(spark, ReferenceEngine(spark), str(out), {**base.META, "dataset": "toy"})
            info = runner.run_kafka(BOOTSTRAP, topic, num_epochs=3, max_offsets_per_trigger=1000, timeout_s=300)
            self.assertEqual([info[str(k)]["events"] for k in range(3)], [6972, 589, 597])
            for k in range(3):
                _, gc, gp, gk = read_stats(str(base.TOY_GOLD / f"epoch{k}.json"))
                _, pc, pp, pk = read_stats(str(out / f"epoch{k}.json"))
                self.assertEqual((gc, gp, gk), (pc, pp, pk), f"epoch {k}")
        finally:
            shutil.rmtree(out, ignore_errors=True)
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
            try:
                admin.delete_topics([topic])
            except Exception:
                pass
            admin.close()


if __name__ == "__main__":
    unittest.main()
