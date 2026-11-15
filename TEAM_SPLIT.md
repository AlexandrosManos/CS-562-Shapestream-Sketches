# Team Work Split — CS-562 Project A: ShapeStream (Team A1 — Sketches)

To avoid merge conflicts in a single notebook and ensure every team member has a clean, modular Git commit history (graded per Section 8), we split our implementation into **5 independent modules** under our own package (e.g., `kit/engine/`, leaving `kit/shapestream/` untouched).

All 5 members can develop and test immediately on **`kit/data/toy`** and run full experiments on **`kit/data/dbpedia` (5% sample)**.

---

## Quick Overview

| Member | Module(s) Owned | Checkpoint 1 (Exact Engine — 20%) | Checkpoint 2 & Final (Variant A1 — 30%) | Report & Presentation |
|---|---|---|---|---|
| **Member 1** | `streaming_runner.py`, `run_experiments.py` | File & Kafka Structured Streaming, partition `checkpoint` barriers, `seq` ordering, `run_info.json` | Automated grid runner (3 runs/config), `evaluate.py` pipeline to `results/results_A.csv` | System architecture, streaming/Kafka ingestion & latency/throughput |
| **Member 2** | `spark_state.py` | Distributed Spark/Parquet state (`entity_types`, `data_triples`), `touched_subjects` & object-type reverse lookup | Bounded-memory state adaptation for A1, sketch serialization & `state_mb()` tracking | Distributed state design, incremental cost & memory footprint (`state_mb`) |
| **Member 3** | `exact_agg.py`, `count_min.py` | Before/After delta aggregation on `touched_subjects`, `Untyped` transitions, cardinality histograms `hist[(C,p)][k]` | **Count-Min Sketch** (`width` in `{2^10, 2^12, 2^14}`, `depth=4`) for value frequencies & cardinality bounds | Exact engine algorithm & Count-Min value frequency estimation |
| **Member 4** | `exact_engine.py`, `hll_sketches.py`, `sketch_engine.py` | `Engine` interface & `ExactEngine` integration (`spark_state` + `exact_agg`), SHACL export check (`to_shacl.py`) | **HyperLogLog** (`p` in `{10, 12, 14}`) for class sizes and supports + **two deletion strategies** (per-epoch HLL merge vs. Count-Min over entity hashes) | HyperLogLog sketch design & error bounds, comparison of the two deletion strategies & step-by-step worked example of one epoch |
| **Member 5** | `tests/test_hard_cases.py`, `benchmark_batch.py`, `plots.py` | Edge-case test suite on `toy` (3 hard cases + `F1=1.0` check) & batch re-extraction (`gold_shapes.py`) timing baseline | Deletion error growth analysis, summary tables & the **3 required plots** from `results/results_A.csv` (`report.tex` figures) | Experimental setup, baseline vs. A1 plots & trade-off discussion |

---

## Detailed Role Breakdown

### Member 1 — Streaming I/O, Kafka Checkpoints & Experiment Runner
- **Owned files:** `kit/engine/streaming_runner.py`, `kit/run_experiments.py`
- **Checkpoint 1 (Exact):**
  - Implement Spark Structured Streaming ingestion for both `file` source (`trigger(availableNow=True)`) and `kafka` source (`foreachBatch`).
  - Handle Kafka `{"op": "checkpoint", "epoch": k}` markers across all partitions before finalizing epoch `k` (ensuring events from `epoch k+1` are not mixed in when running `replay_to_kafka.py --rate 0`).
  - Enforce `seq` ordering per subject within micro-batches and write `epoch<k>.json` (`write_stats`) and `run_info.json` (`latency_s`, `events_per_s`, `state_mb`).
- **Checkpoint 2 & Final (Variant A1):**
  - Build the experiment automation runner across the full parameter grid (mean of 3 runs per configuration on the same machine, logging machine specs & Spark settings) and append metrics via `python -m shapestream.evaluate ... --results results/results_A.csv`.
- **Report & Presentation:** Problem setup, streaming/Kafka architecture, and throughput/latency measurements.

### Member 2 — Distributed Spark State & Touched-Entity Resolution
- **Owned files:** `kit/engine/spark_state.py`
- **Checkpoint 1 (Exact):**
  - Maintain the large KG state in Spark (Parquet/Delta or stateful partitions, never collecting all entities to the driver): active `entity_types(s, cls)` and `data_triples(s, p, o, okind, dt)`.
  - Compute `touched_subjects` per micro-batch: subjects directly modified in the batch **plus** subjects pointing to any IRI object `o` whose `rdf:type` changed in the batch.
  - Manage Spark lineage truncation across micro-batches and implement accurate `state_mb()` measurement.
- **Checkpoint 2 & Final (Variant A1):**
  - Adapt state storage for Variant A1 so exact per-class/constraint structures are replaced by compact sketches, and track `state_mb` across all grid configurations.
- **Report & Presentation:** Distributed Spark state management, incremental update cost, and memory footprint analysis.

### Member 3 — Exact Incremental Aggregator & Count-Min Value Frequencies
- **Owned files:** `kit/engine/exact_agg.py`, `kit/engine/count_min.py`
- **Checkpoint 1 (Exact):**
  - Implement the Before/After delta updater over `touched_subjects` for `classes[C]`, `urn:shapestream:Untyped` transitions, and `constraints[(C, p, T)]`.
  - Maintain per-`(C, p)` multiplicity histograms `hist[(C, p)][k]` so `support(C, p)`, `min_count` (`0` or `1`), and `max_count` update in $O(1)$ even when deletions lower the maximum.
- **Checkpoint 2 & Final (Variant A1):**
  - Implement **Count-Min Sketches** (`width` $\in \{2^{10}, 2^{12}, 2^{14}\}$, `depth = 4`) replacing exact value frequencies / per-entity property multiplicities, and integrate cardinality estimation into Variant A1.
- **Report & Presentation:** Exact incremental update rules and Count-Min sketch design/accuracy.

### Member 4 — Engine Integration, HyperLogLog & Deletion Strategies
- **Owned files:** `kit/engine/exact_engine.py`, `kit/engine/hll_sketches.py`, `kit/engine/sketch_engine.py`
- **Checkpoint 1 (Exact):**
  - Define the common `Engine` base interface (`process_batch`, `current_stats`, `state_mb`) and wire `ExactEngine` (`spark_state` + `exact_agg`), verifying end-to-end SHACL generation (`shapestream.to_shacl`).
- **Checkpoint 2 & Final (Variant A1):**
  - Replace exact distinct counts (`|C|`, `support(C, p)`, `support(C, p, T)`) with **HyperLogLog** sketches (`p` $\in \{10, 12, 14\}$).
  - Implement and compare **two HLL deletion strategies** (since standard HLL is insert-only):
    1. *Per-epoch HLL merge:* one HLL sketch per epoch, merged across epochs.
    2. *Count-Min over entity hashes:* conservative-update / counter-based sketch supporting decrements on deletions.
- **Report & Presentation:** HyperLogLog sketch design & error bounds, comparison of the two deletion strategies, and the required **step-by-step worked example of one epoch** in the report and presentation.

### Member 5 — Verification Suite, Batch Benchmarking & Evaluation Plots
- **Owned files:** `kit/tests/test_hard_cases.py`, `kit/scripts_team/benchmark_batch.py`, `kit/scripts_team/plots.py`, `report.tex` (figures/tables)
- **Checkpoint 1 (Exact):**
  - Build synthetic & `toy` KG unit tests targeting the 3 hard cases: (1) deletions lowering `max_count` or removing the last property value, (2) subject `rdf:type` changes (`Untyped` $\leftrightarrow C$), and (3) object `rdf:type` changes (`IRI` $\leftrightarrow$ class).
  - Verify `F1 = 1.0` and `support_mape = 0.0` on `toy` & `dbpedia`, and measure batch re-extraction latency (`gold_shapes.py`) per epoch as the non-incremental baseline.
- **Checkpoint 2 & Final (Variant A1):**
  - Process `results/results_A.csv` (computing 3-run means/std), quantify how estimation error grows with deletions across epochs, and generate the **3 required publication-ready plots** (Section 5) for `report.tex`:
    1. `F1` at $\sigma = 10, \tau = 0.1$ per epoch,
    2. `support MAPE` vs. `state_mb` across the HLL/CM grid,
    3. `latency_s` per epoch for Exact Engine, Variant A1, and batch re-extraction.
- **Report & Presentation:** Experimental protocol, batch vs. incremental speedups, and accuracy-vs.-memory trade-off plots.

---

## Collaboration & Git Rules
1. **Never modify `kit/shapestream/`** and **never commit `kit/data/`**.
2. Keep `ExactEngine` and `SketchEngine` compliant with the same 3-method interface (`process_batch(batch_df, batch_id)`, `current_stats()`, `state_mb()`) so `streaming_runner.py` runs both unchanged.
3. Use feature branches / PRs per module and commit regularly so every member's contributions are clearly visible in Git history.
