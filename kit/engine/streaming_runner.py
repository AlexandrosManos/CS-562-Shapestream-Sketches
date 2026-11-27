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
the sketch engine, `hll_p`, `cm_width`, `cm_depth` and `deletions`. For each
epoch k the runner calls:

    process_batch(batch_df, batch_id)   once per non-empty micro-batch of epoch k
    on_epoch_end(k)                     if the engine defines it
    current_stats()                     -> (classes, properties, constraints)
    state_mb()                          -> float or None

batch_df has the EVENT_SCHEMA columns (seq included), only op in {'+', '-'},
and only events of epoch k. It is hash-partitioned by subject and sorted by
(s, seq) inside each partition, so the events of a subject are in seq order.
batch_id grows across the whole run (it does not restart at every epoch).

The Kafka source (replay_to_kafka.py) is not implemented yet.
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


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--stream", required=True, help="e.g. data/toy/streams/full")
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
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    a = parse_args(argv)
    dataset = a.dataset or dataset_name(a.stream)
    config = config_name(a.engine, a.hll_p, a.cm_width, a.deletions)
    out_dir = a.out or os.path.join("runs", TEAM, dataset, config)

    params: dict[str, Any] = {"state_dir": os.path.join(out_dir, "_state")}
    if a.engine == "sketch":
        params.update(hll_p=a.hll_p, cm_width=a.cm_width, cm_depth=a.cm_depth, deletions=a.deletions)

    spark = get_spark(f"streaming_runner-{config}", a.driver_memory)
    engine = make_engine(a.engine, spark, **params)
    runner = StreamingRunner(spark, engine, out_dir,
                             {"team": TEAM, "variant": a.variant or config, "dataset": dataset},
                             order_by_seq=not a.no_seq_order,
                             max_files_per_trigger=a.max_files_per_trigger)
    runner.run_files(a.stream)
    print(f"wrote {out_dir}/epoch*.json and {out_dir}/run_info.json")


if __name__ == "__main__":
    main()
