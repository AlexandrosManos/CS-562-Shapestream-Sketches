# Module Contracts & Mock Walkthrough (Team A1)

Concrete mock inputs and expected outputs across **Epochs 0, 1, and 2** covering all 3 hard cases in isolation:
- **Epoch 0 (Initial load):** Inserts `paper0`, `paper1` (with 2 authors -> `max_count = 2`), and untyped `s2`. Both `v0` and `v1` are untyped IRI objects (`urn:shapestream:IRI`).
- **Epoch 1 (Hard Cases 1 & 2 + untouched isolation):**
  - *Hard Case 1:* `paper1` loses `"Bob"`, dropping `(Article, author)` `max_count` from `2` to `1`.
  - *Hard Case 2:* `s2` gains `rdf:type Article`, moving out of `urn:shapestream:Untyped` into `Article`.
  - *Incremental isolation:* `paper0` (and `v0`) have zero events, so `paper0` is **not** in `2_touched_slices_epoch1.json`, yet its counts remain intact.
- **Epoch 2 (Isolated Hard Case 3 — Object `rdf:type` change only):**
  - Single event: `+(v0, rdf:type, Conference)`. **`paper0` has NO direct events in Epoch 2**, yet because `paper0` has an active `(paper0, venue, v0)` edge, reverse lookup MUST include `paper0` in `touched_subjects`, moving `(Article, venue, urn:shapestream:IRI)` to `(Article, venue, Conference)`.

## Critical Contract Rules

1. **Definition of `touched_subjects` (`spark_state.py` — Member 2):**
   For every micro-batch, `touched_subjects` MUST equal:
   $$\text{touched\_subjects} = \{s \text{ appearing in } \text{batch\_df}\} \;\cup\; \{s \mid (s, p, o) \text{ is active in } \text{data\_triples} \land o \text{ gained or lost an } \text{rdf:type} \text{ in } \text{batch\_df}\}$$
   Maintain a reverse lookup (`o -> s` on IRI objects in `data_triples`) so Hard Case 3 runs without scanning untouched literals/subjects.

2. **Epoch boundaries & Kafka micro-batches (`streaming_runner.py` — Member 1):**
   - With `file` source (`availableNow=True` per `epoch=k` folder), call `engine.process_batch(batch_df, id)` for each micro-batch, then `engine.on_epoch_end(k)` and `engine.current_stats()`.
   - With `kafka` source (`replay_to_kafka.py --rate 0`), a single `foreachBatch` micro-batch may span the end of epoch `k`, `{"op": "checkpoint", "epoch": k}` markers, and the start of epoch `k+1`. `streaming_runner.py` MUST split by `epoch` in ascending order: process `op in {'+', '-'}` rows for epoch `k`, and as soon as `checkpoint` for `k` has been seen across **all** Kafka partitions, call `engine.on_epoch_end(k)` and `engine.current_stats()` **before** feeding epoch `k+1` rows into `process_batch`.

## Which file is your Input / Output?

| Member | Module | Input | Expected Output |
|---|---|---|---|
| **Member 1** | `streaming_runner.py` | `1_stream_events_epoch{0,1,2}.jsonl` (File or Kafka) | Calls `process_batch`, `on_epoch_end(k)`, writes `3_expected_stats_epoch{0,1,2}.json` + `run_info.json` |
| **Member 2** | `spark_state.py` | `batch_df` (`EVENT_SCHEMA` from `1_stream_events_epoch*.jsonl`) | `(before_slice, after_slice)` as `TouchedSlice` DataFrames matching `2_touched_slices_epoch{0,1,2}.json` |
| **Member 3** | `exact_agg.py` | `(before_slice, after_slice)` (`TouchedSlice` from `2_touched_slices_epoch*.json`) | `current_stats()` tuple `(classes, properties, constraints)` matching `3_expected_stats_epoch{0,1,2}.json` |
| **Member 4** | `exact_engine.py` | `batch_df` from Member 1 | Wires Member 2 (`spark_state`) -> Member 3 (`exact_agg`), exposes `BaseEngine` (`contracts.py`) |
| **Member 5** | `tests/test_hard_cases.py` | Feeds `1_stream_events_epoch{0,1,2}.jsonl` into `ExactEngine` | Asserts `ev.evaluate(expected, pred)` reaches `F1 = 1.0`, `support_mape = 0.0`, `cardinality_accuracy = 1.0` |

## Shared Spark Schemas (`kit/engine/contracts.py`)

Each `TouchedSlice` holds 3 Spark DataFrames restricted **only** to `touched_subjects`:
1. `types_df`: `(s: string, cls: string)` — active `rdf:type` IRIs of touched subjects (omit untyped subjects; `exact_agg.py` assigns `urn:shapestream:Untyped`).
2. `data_df`: `(s: string, p: string, o: string, okind: string, dt: string)` — active non-`rdf:type` triples of touched subjects.
3. `obj_types_df`: `(o: string, cls: string)` — active `rdf:type` IRIs of IRI objects referenced by `data_df` (omit untyped IRI objects; `exact_agg.py` assigns `urn:shapestream:IRI`).
