"""Streaming runner (Member 1): drives an engine over the change stream.

Reads the events epoch by epoch with Spark Structured Streaming, feeds them to
an engine that follows `engine.contracts.BaseEngine`, and at the end of every
epoch writes `epoch<k>.json` (shapestream.common.write_stats) and updates
`run_info.json` ({"<k>": {latency_s, events_per_s, state_mb}}).

Run from the kit folder (inside the course container):

    python -m engine.streaming_runner --stream data/toy/streams/full --engine reference
    python -m engine.streaming_runner --stream data/dblp/streams/full --engine exact
    python -m engine.streaming_runner --stream data/dblp/streams/full --engine sketch --hll-p 12 --cm-width 4096

Output goes to runs/A1/<dataset>/<config>/ unless --out is given, with
<config> = "exact", "reference" or "sketch_p<P>_w<W>[_<deletions>]", so every
grid point has its own folder. The same string is written as meta.variant, so
the rows that evaluate.py appends to results_A.csv can be told apart.

Engine contract: an engine class is built as
`EngineClass(spark, **params)`, where params always holds `state_dir` and, for
the sketch engine, `hll_p`, `cm_width`, `cm_depth` and `deletions`.
state_dir is <out>/_state, and the runner empties it before every run, so
state from an earlier run never stays on disk. Removing old state while the
run goes on is the engine's job. For each epoch k the runner calls:

    process_batch(batch_df, batch_id)   once per non-empty micro-batch of epoch k
    on_epoch_end(k)                     if the engine defines it
    current_stats()                     -> (classes, properties, constraints)
    state_mb()                          -> float or None

batch_df has the EVENT_SCHEMA columns (seq included), only op in {'+', '-'},
and only events of epoch k. It is hash-partitioned by subject and sorted by
(s, seq) inside each partition, so the events of a subject are in seq order.
batch_id grows across the whole run (it does not restart at every epoch).

Kafka source (--source kafka) reads the topic written by
shapestream.replay_to_kafka. One micro-batch can hold the end of epoch k, the
{"op": "checkpoint", "epoch": k} markers and the start of epoch k+1, and the
partitions do not advance together. So the runner feeds only the rows of the
open epoch k, stages rows of later epochs in <out>/_pending/, and closes epoch
k once its marker has arrived on every partition of the topic. Then it feeds
the staged rows of k+1 before any newer ones. The topic keeps old replays, so
use a fresh topic for every run:

    python -m shapestream.replay_to_kafka --stream data/toy/streams/full --topic kg-toy-run1 --rate 0
    python -m engine.streaming_runner --source kafka --topic kg-toy-run1 --stream data/toy/streams/full --engine reference

With Kafka, --stream only gives the number of epochs and the dataset name, and
the latency of epoch k runs from the first micro-batch that carries epoch k (or
from the close of epoch k-1, if that is later) until epoch<k>.json is written.
"""
from __future__ import annotations

import argparse
import glob
import importlib
import json
import os
import shutil
import sys
import time
from collections import defaultdict
from typing import Any, Optional

KIT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if KIT_DIR not in sys.path:
    sys.path.insert(0, KIT_DIR)

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from shapestream.common import write_stats  # noqa: E402
from shapestream.spark_io import EVENT_SCHEMA, get_spark  # noqa: E402

TEAM = "A1"

# --engine name -> (module, class). Modules are imported only when selected,
# so the runner works before exact_engine.py / sketch_engine.py exist.
ENGINES = {
    "reference": ("engine.reference_engine", "ReferenceEngine"),
    "exact": ("engine.exact_engine", "ExactEngine"),
    "sketch": ("engine.sketch_engine", "SketchEngine"),
}


def make_engine(name: str, spark: SparkSession, **params: Any):
    """Build the engine selected with --engine (see the module docstring)."""
    if name not in ENGINES:
        raise ValueError(f"unknown engine {name!r}; choose one of {sorted(ENGINES)}")
    module, cls_name = ENGINES[name]
    try:
        mod = importlib.import_module(module)
    except ModuleNotFoundError as e:
        if e.name == module:
            raise SystemExit(f"--engine {name}: {module.replace('.', '/')}.py does not exist yet") from e
        raise
    return getattr(mod, cls_name)(spark, **params)


def config_name(engine: str, hll_p: Optional[int] = None, cm_width: Optional[int] = None,
                deletions: Optional[str] = None) -> str:
    """Folder / variant name of one configuration, e.g. 'exact' or 'sketch_p12_w4096'."""
    if engine != "sketch":
        return engine
    name = f"sketch_p{hll_p}_w{cm_width}"
    return f"{name}_{deletions}" if deletions else name


def epoch_dirs(stream_dir: str) -> list[tuple[int, str]]:
    """The epoch=<k> folders of a stream, in ascending epoch order."""
    dirs = glob.glob(os.path.join(stream_dir, "epoch=*"))
    out = sorted((int(d.rsplit("=", 1)[1]), d) for d in dirs if os.path.isdir(d))
    if not out:
        raise FileNotFoundError(f"no epoch=<k> folders in {stream_dir}")
    return out


def dataset_name(stream_dir: str) -> str:
    """data/<dataset>/streams/<name> -> <dataset>; otherwise the folder name."""
    parts = os.path.normpath(os.path.abspath(stream_dir)).split(os.sep)
    if len(parts) >= 3 and parts[-2] == "streams":
        return parts[-3]
    return parts[-1]


def fresh_state_dir(out_dir: str) -> str:
    """Empty <out>/_state before a run and return its path for the engine."""
    state_dir = os.path.join(out_dir, "_state")
    shutil.rmtree(state_dir, ignore_errors=True)
    os.makedirs(state_dir)
    return state_dir


def kafka_partitions(bootstrap: str, topic: str) -> set[int]:
    """Partition ids of a topic. The replayer sends a marker to each of them."""
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(bootstrap_servers=bootstrap)
    try:
        parts = consumer.partitions_for_topic(topic)
    finally:
        consumer.close()
    if not parts:
        raise RuntimeError(f"topic {topic!r} not found at {bootstrap}")
    return set(parts)


class StreamingRunner:
    """Feeds one engine epoch by epoch and writes the statistics and run info."""

    def __init__(self, spark: SparkSession, engine, out_dir: str, meta: dict,
                 order_by_seq: bool = True, max_files_per_trigger: Optional[int] = None) -> None:
        self.spark = spark
        self.engine = engine
        self.out_dir = out_dir
        self.meta = dict(meta)
        self.order_by_seq = order_by_seq
        self.max_files_per_trigger = max_files_per_trigger
        self.next_batch_id = 0
        self.run_info: dict[str, dict] = {}
        os.makedirs(out_dir, exist_ok=True)
        # Fresh streaming checkpoints on every run: with an old checkpoint the
        # file source thinks the files were already processed and reads nothing.
        self.chk_root = os.path.join(out_dir, "_chk")
        shutil.rmtree(self.chk_root, ignore_errors=True)
        # Kafka rows of epochs that are not open yet wait here as Parquet.
        self.pending_root = os.path.join(out_dir, "_pending")

    # Shared by every source
    def _prepare(self, events: DataFrame) -> DataFrame:
        """Keep the change events and put each subject's events in seq order."""
        events = events.filter(F.col("op").isin("+", "-"))
        if self.order_by_seq:
            events = events.repartition("s").sortWithinPartitions("s", "seq")
        return events

    def feed(self, events: DataFrame) -> int:
        """Pass one micro-batch of a single epoch to the engine; returns its size."""
        events = self._prepare(events).persist()
        try:
            n = events.count()
            if n:
                self.engine.process_batch(events, self.next_batch_id)
                self.next_batch_id += 1
        finally:
            events.unpersist()
        return n

    def finish_epoch(self, epoch: int, n_events: int, t0: float) -> dict:
        """Close an epoch: on_epoch_end, write epoch<k>.json, update run_info.json."""
        on_end = getattr(self.engine, "on_epoch_end", None)
        if callable(on_end):
            on_end(epoch)
        classes, properties, constraints = self.engine.current_stats()
        write_stats(os.path.join(self.out_dir, f"epoch{epoch}.json"),
                    {**self.meta, "epoch": epoch}, classes, properties, constraints)
        latency = time.perf_counter() - t0
        state_mb = self.engine.state_mb()
        info = {
            "latency_s": round(latency, 3),
            "events_per_s": round(n_events / latency) if latency > 0 else None,
            "state_mb": None if state_mb is None else round(float(state_mb), 3),
            "events": n_events,
        }
        self.run_info[str(epoch)] = info
        self._write_run_info()
        print(f"epoch {epoch}: {n_events} events in {latency:.2f}s "
              f"({info['events_per_s']} events/s, state {info['state_mb']} MB)", flush=True)
        return info

    def _write_run_info(self) -> None:
        path = os.path.join(self.out_dir, "run_info.json")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.run_info, f, indent=1)
        os.replace(tmp, path)

    # File source: one availableNow query per epoch folder
    def run_files(self, stream_dir: str) -> dict:
        for epoch, path in epoch_dirs(stream_dir):
            n_events = 0

            def on_batch(batch_df: DataFrame, _query_batch_id: int) -> None:
                nonlocal n_events
                n_events += self.feed(batch_df)

            reader = self.spark.readStream.schema(EVENT_SCHEMA)
            if self.max_files_per_trigger:
                reader = reader.option("maxFilesPerTrigger", self.max_files_per_trigger)
            t0 = time.perf_counter()
            query = (reader.json(path)
                     .writeStream.foreachBatch(on_batch)
                     .option("checkpointLocation", os.path.join(self.chk_root, f"epoch={epoch}"))
                     .trigger(availableNow=True)
                     .start())
            query.awaitTermination()
            self.finish_epoch(epoch, n_events, t0)
        return self.run_info

    # Kafka source: one query for the whole run, epochs split by the markers
    def run_kafka(self, bootstrap: str, topic: str, num_epochs: int, first_epoch: int = 0,
                  partitions: Optional[set[int]] = None, starting_offsets: str = "earliest",
                  max_offsets_per_trigger: Optional[int] = None,
                  timeout_s: Optional[float] = None) -> dict:
        self.reset_kafka(num_epochs, first_epoch, partitions, bootstrap, topic)
        reader = (self.spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", bootstrap)
                  .option("subscribe", topic)
                  .option("startingOffsets", starting_offsets))
        if max_offsets_per_trigger:
            reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        events = (reader.load()
                  .select("partition",
                          F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
                  .select("partition", "e.*"))
        query = (events.writeStream.foreachBatch(self.on_kafka_batch)
                 .option("checkpointLocation", os.path.join(self.chk_root, "kafka"))
                 .start())
        deadline = time.monotonic() + timeout_s if timeout_s else None
        try:
            while not self.kafka_done:
                if query.awaitTermination(1):
                    break
                if deadline is not None and time.monotonic() > deadline:
                    raise TimeoutError(
                        f"stopped after {timeout_s}s: epoch {self.current_epoch} is still open, "
                        f"markers seen on partitions {sorted(self.markers[self.current_epoch])}")
        finally:
            query.stop()
        if not self.kafka_done:
            raise RuntimeError(f"the Kafka query ended with epoch {self.current_epoch} still open")
        shutil.rmtree(self.pending_root, ignore_errors=True)
        return self.run_info

    def reset_kafka(self, num_epochs: int, first_epoch: int = 0,
                    partitions: Optional[set[int]] = None,
                    bootstrap: Optional[str] = None, topic: Optional[str] = None) -> None:
        """Start a Kafka run: `first_epoch` is open and nothing is staged."""
        self.current_epoch = first_epoch
        self.last_epoch = first_epoch + num_epochs - 1
        self.partitions = set(partitions) if partitions else None
        self.kafka_source = (bootstrap, topic)
        self.markers: dict[int, set[int]] = defaultdict(set)
        self.epoch_events: dict[int, int] = defaultdict(int)
        self.first_seen: dict[int, float] = {}
        self.closed_at: dict[int, float] = {}
        self.staged: set[int] = set()
        self.all_closed = False
        self.kafka_done = False
        shutil.rmtree(self.pending_root, ignore_errors=True)

    def on_kafka_batch(self, batch_df: DataFrame, _query_batch_id: int) -> None:
        """foreachBatch of the Kafka query: split one micro-batch by epoch."""
        if self.all_closed:
            return
        started = time.perf_counter()
        batch = batch_df.persist()
        try:
            summary = batch.groupBy("epoch", "op", "partition").count().collect()
            event_epochs = set()
            for r in summary:
                if r["epoch"] is None:
                    continue
                self.first_seen.setdefault(r["epoch"], started)
                if r["op"] == "checkpoint":
                    self.markers[r["epoch"]].add(r["partition"])
                else:
                    event_epochs.add(r["epoch"])
            late = sorted(e for e in event_epochs if e < self.current_epoch)
            if late:
                raise RuntimeError(f"events of closed epoch(s) {late} arrived; "
                                   "replay into a fresh topic")

            events = batch.filter(F.col("op") != "checkpoint").select(*EVENT_SCHEMA.fieldNames())
            for epoch in sorted(event_epochs):
                rows = events.filter(F.col("epoch") == epoch)
                if epoch == self.current_epoch:
                    self.epoch_events[epoch] += self.feed(rows)
                else:
                    rows.write.mode("append").parquet(self._pending_dir(epoch))
                    self.staged.add(epoch)

            if self.partitions is None and summary:
                self.partitions = kafka_partitions(*self.kafka_source)
            self._close_ready_epochs()
        finally:
            batch.unpersist()
        self.kafka_done = self.all_closed

    def _close_ready_epochs(self) -> None:
        """Close the open epoch while its marker has arrived on every partition."""
        while self.partitions and self.markers[self.current_epoch] >= self.partitions:
            epoch = self.current_epoch
            t0 = max(self.first_seen[epoch], self.closed_at.get(epoch - 1, float("-inf")))
            self.finish_epoch(epoch, self.epoch_events[epoch], t0)
            self.closed_at[epoch] = time.perf_counter()
            shutil.rmtree(self._pending_dir(epoch), ignore_errors=True)
            if epoch == self.last_epoch:
                self.all_closed = True
                return
            self.current_epoch = epoch + 1
            if self.current_epoch in self.staged:
                staged = self.spark.read.parquet(self._pending_dir(self.current_epoch))
                self.epoch_events[self.current_epoch] += self.feed(staged)

    def _pending_dir(self, epoch: int) -> str:
        return os.path.join(self.pending_root, f"epoch{epoch}")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--source", choices=["file", "kafka"], default="file")
    ap.add_argument("--stream", help="e.g. data/toy/streams/full (required for the file source)")
    ap.add_argument("--dataset", help="dataset name in meta (default: taken from --stream)")
    ap.add_argument("--engine", choices=sorted(ENGINES), default="exact")
    ap.add_argument("--hll-p", type=int, default=12, help="sketch engine: HLL precision")
    ap.add_argument("--cm-width", type=int, default=1 << 12, help="sketch engine: Count-Min width")
    ap.add_argument("--cm-depth", type=int, default=4, help="sketch engine: Count-Min depth")
    ap.add_argument("--deletions", default=None, help="sketch engine: deletion strategy name")
    ap.add_argument("--out", help="output folder (default: runs/A1/<dataset>/<config>)")
    ap.add_argument("--variant", help="meta.variant (default: the config name)")
    ap.add_argument("--max-files-per-trigger", type=int, default=None)
    ap.add_argument("--no-seq-order", action="store_true",
                    help="skip the per-subject seq sort of each micro-batch")
    ap.add_argument("--driver-memory", default=None)
    kafka = ap.add_argument_group("Kafka source")
    kafka.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    kafka.add_argument("--topic", default="kg-changes")
    kafka.add_argument("--num-epochs", type=int, help="epochs to read (default: from --stream)")
    kafka.add_argument("--first-epoch", type=int, default=0)
    kafka.add_argument("--partitions", type=int, help="partitions of the topic (default: ask Kafka)")
    kafka.add_argument("--starting-offsets", default="earliest")
    kafka.add_argument("--max-offsets-per-trigger", type=int, default=None)
    kafka.add_argument("--timeout", type=float, default=None, help="give up after this many seconds")
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    a = parse_args(argv)
    if a.source == "file" and not a.stream:
        raise SystemExit("--source file needs --stream")
    num_epochs = a.num_epochs or (len(epoch_dirs(a.stream)) if a.stream else None)
    if a.source == "kafka" and not num_epochs:
        raise SystemExit("--source kafka needs --stream or --num-epochs")
    dataset = a.dataset or (dataset_name(a.stream) if a.stream else None)
    if not dataset:
        raise SystemExit("give --dataset when there is no --stream")
    config = config_name(a.engine, a.hll_p, a.cm_width, a.deletions)
    out_dir = a.out or os.path.join("runs", TEAM, dataset, config)

    params: dict[str, Any] = {"state_dir": fresh_state_dir(out_dir)}
    if a.engine == "sketch":
        params.update(hll_p=a.hll_p, cm_width=a.cm_width, cm_depth=a.cm_depth, deletions=a.deletions)

    spark = get_spark(f"streaming_runner-{config}", a.driver_memory)
    engine = make_engine(a.engine, spark, **params)
    runner = StreamingRunner(spark, engine, out_dir,
                             {"team": TEAM, "variant": a.variant or config, "dataset": dataset},
                             order_by_seq=not a.no_seq_order,
                             max_files_per_trigger=a.max_files_per_trigger)
    if a.source == "file":
        runner.run_files(a.stream)
    else:
        runner.run_kafka(a.bootstrap, a.topic, num_epochs, a.first_epoch,
                         set(range(a.partitions)) if a.partitions else None,
                         a.starting_offsets, a.max_offsets_per_trigger, a.timeout)
    print(f"wrote {out_dir}/epoch*.json and {out_dir}/run_info.json")


if __name__ == "__main__":
    main()
