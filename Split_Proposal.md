# Team Work Split — CS-562 Project A: ShapeStream (Team A1 — Sketches)

To avoid merge conflicts in a single notebook and ensure every team member has a clean, balanced Git commit history (graded per Section 8), we split our implementation into **5 independent modules** under our own package (e.g., `kit/engine/`, leaving `kit/shapestream/` untouched).

All 5 members can develop and test immediately on **`kit/data/toy`** (already built) before running full experiments on **`kit/data/dbpedia` (5% sample)**.

---

## Quick Overview

| Member | Module(s) Owned | Checkpoint 1 (Exact Engine — 20%) | Checkpoint 2 & Final (Variant A1 — 30%) | Report (LNCS, 6p) & Presentation (15m = 5×3m) |
|---|---|---|---|---|
| **Member 1** | `streaming_runner.py` | File & Kafka Structured Streaming, partition `checkpoint` barriers, `seq` ordering, `run_info.json` | Automated grid runner (3 runs/config), `evaluate.py` pipeline to `results/results_A.csv` | System architecture, streaming/Kafka ingestion & latency/throughput |
| **Member 2** | `spark_state.py` | Distributed Spark/Parquet state (`entity_types`, `data_triples`), `touched_subjects` & object-type reverse lookup, `state_mb()` | Bounded-memory state adaptation for A1, sketch serialization & memory footprint tracking | Distributed state design, incremental cost & memory analysis (`state_mb`) |
| **Member 3** | `exact_agg.py`, `count_min.py` | Before/After delta aggregation on `touched_subjects`, `Untyped` transitions, cardinality histograms `hist[(C,p)][k]` | **Count-Min Sketch** (`width` in `{2^10, 2^12, 2^14}`, `depth=4`) for value frequencies & cardinality bounds | Exact engine algorithm & Count-Min value frequency estimation |
| **Member 4** | `tests/`, `hll_strategy1.py` | Unit & edge-case tests on `toy` (deletions lowering `max_count`, subject & object `rdf:type` changes, `F1=1.0` check) | **HyperLogLog** (`p` in `{10, 12, 14}`) for `|C|` and supports + **Deletion Strategy #1** (per-epoch HLL merge) | HLL design, Deletion Strategy #1 & worked epoch example (presentation) |
| **Member 5** | `benchmarks/`, `hll_strategy2.py`, `plots.py` | Setup verification (`SETUP_REPORT.txt`), Exact vs. batch (`gold_shapes.py`) timing baseline, `report.tex` setup | **Deletion Strategy #2** (Count-Min conservative update over entity hashes), deletion error growth & 3 required plots | Deletion Strategy #2, experimental grid trade-offs & plots |

---

## Detailed Role Breakdown

### Member 1 — Streaming I/O, Kafka Checkpoints & Experiment Runner
- **Owned files:** `kit/engine/streaming_runner.py`, `kit/run_experiments.py`
- **Checkpoint 1 (Exact):**
  - Implement Spark Structured Streaming ingestion for both `file` source (`trigger(availableNow=True)`) and `kafka` source (`foreachBatch`).
  - Handle Kafka `{"op": "checkpoint", "epoch": k}` markers across all partitions before finalizing epoch `k` (ensuring events from `epoch k+1` are not mixed in when running `replay_to_kafka.py --rate 0`).
  - Enforce `seq` ordering per subject within micro-batches and write `epoch<k>.json` (`write_stats`) and `run_info.json` (`latency_s`, `events_per_s`, `state_mb`).
- **Checkpoint 2 & Final (Variant A1):**
  - Build the experiment automation runner across the full parameter grid (3 runs per configuration on the same machine, logging machine specs & Spark settings) and append metrics via `python -m shapestream.evaluate ... --results results/results_A.csv`.
- **Report & Presentation:** Section 1–2 (Problem setup, streaming/Kafka architecture, latency & throughput comparison).

### Member 2 — Distributed Spark State & Touched-Entity Resolution
- **Owned files:** `kit/engine/spark_state.py`
- **Checkpoint 1 (Exact):**
  - Maintain the large KG state in Spark (Parquet/Delta or stateful partitions, never collecting all entities to the driver): active `entity_types(s, cls)` and `data_triples(s, p, o, okind, dt)`.
  - Compute `touched_subjects` per micro-batch: subjects directly modified in the batch **plus** subjects pointing to any IRI object `o` whose `rdf:type` changed in the batch.
  - Manage Spark lineage truncation across micro-batches and implement accurate `state_mb()` measurement.
- **Checkpoint 2 & Final (Variant A1):**
  - Adapt state storage for Variant A1 so exact per-class/constraint entity sets are replaced by compact sketches, and measure exact `state_mb` across the grid.
- **Report & Presentation:** Distributed Spark state management, handling the 3 hard cases incrementally, and state size breakdown.

### Member 3 — Exact Incremental Aggregator & Count-Min Value Frequencies
- **Owned files:** `kit/engine/exact_agg.py`, `kit/engine/count_min.py`
- **Checkpoint 1 (Exact):**
  - Implement the Before/After delta updater over `touched_subjects` for `classes[C]`, `urn:shapestream:Untyped` transitions, and `constraints[(C, p, T)]`.
  - Maintain per-`(C, p)` multiplicity histograms `hist[(C, p)][k]` so `support(C, p)`, `min_count` (`0` or `1`), and `max_count` update in $O(1)$ even when deletions lower the maximum.
- **Checkpoint 2 & Final (Variant A1):**
  - Implement **Count-Min Sketches** (`width` $\in \{2^{10}, 2^{12}, 2^{14}\}$, `depth = 4`) replacing exact value frequencies / per-entity property multiplicities, and integrate cardinality estimation into Variant A1.
- **Report & Presentation:** Exact incremental update rules and Count-Min sketch design/accuracy.

### Member 4 — Verification Suite, HyperLogLog & Deletion Strategy #1
- **Owned files:** `kit/tests/test_exact.py`, `kit/engine/hll_strategy1.py`
- **Checkpoint 1 (Exact):**
  - Build synthetic & `toy` KG tests specifically targeting the 3 hard cases: (1) deletions lowering `max_count` or removing the last property value, (2) subject `rdf:type` additions/removals (`Untyped` $\leftrightarrow C$), and (3) object `rdf:type` changes updating `T` (`IRI` $\leftrightarrow$ class).
  - Verify `F1 = 1.0`, `support_mape = 0.0`, and `cardinality_accuracy = 1.0` across all epochs and thresholds.
- **Checkpoint 2 & Final (Variant A1):**
  - Replace exact distinct counts (`|C|`, `support(C, p)`, `support(C, p, T)`) with **HyperLogLog** sketches (`p` $\in \{10, 12, 14\}$).
  - Implement **Deletion Strategy #1** (e.g., per-epoch HLL sketches merged across active epochs).
- **Report & Presentation:** HLL formulation, Deletion Strategy #1, and the required **worked example of one epoch** in the presentation.

### Member 5 — Baseline Benchmarking, Deletion Strategy #2 & Plots
- **Owned files:** `kit/engine/hll_strategy2.py`, `kit/scripts_team/benchmark_batch.py`, `kit/scripts_team/plots.py`, `report.tex`
- **Checkpoint 1 (Exact):**
  - Complete environment/dataset verification (`SETUP_REPORT.txt`) and benchmark `gold_shapes.py` batch re-extraction time per epoch vs. the Exact Incremental Engine.
  - Set up the 6-page LNCS `report.tex` structure and bibliography.
- **Checkpoint 2 & Final (Variant A1):**
  - Implement **Deletion Strategy #2** for distinct counting under deletions (e.g., Count-Min sketch with conservative update over hashed entity buckets / HLL register counters).
  - Analyze how estimation error grows with deletions across epochs and generate the **3 required plots** (Section 5):
    1. `F1` at $\sigma = 10, \tau = 0.1$ per epoch,
    2. `support MAPE` vs. `state_mb` across the HLL/CM grid,
    3. `latency_s` per epoch for Exact Engine, Variant A1, and batch re-extraction.
- **Report & Presentation:** Deletion Strategy #2, head-to-head comparison of the two deletion strategies, and experimental results/discussion.

---

## Collaboration & Git Rules
1. **Never modify `kit/shapestream/`** and **never commit `kit/data/`**.
2. Agree on the `Engine` interface (`process_batch(batch_df, batch_id)`, `current_stats()`, `state_mb()`) on Day 1 so `ExactEngine`, `SketchEngineS1`, and `SketchEngineS2` are drop-in interchangeable in `streaming_runner.py`.
3. Use feature branches / PRs per module and commit regularly so every member's contributions are clearly visible in Git history.
