# Project A kit: ShapeStream

HY562 / CS-562 Fall 2026. Everything every Project A team needs: scripts that build the data, change streams
and gold shapes, the evaluation script and a starter notebook. Each team sets everything up itself: start with
`SETUP.md`. All teams use this kit unchanged, so that results can be compared.
The project description is `../CS562_ProjectA_2026.pdf`.

## Contents

| Path | What it is |
|---|---|
| `ProjectA_starter.ipynb` | Starter notebook: read the stream, plug in your engine, write statistics, evaluate |
| `shapestream/common.py` | Shared definitions: special IRIs, N-Triples parser, event format, statistics format |
| `shapestream/spark_io.py` | Spark session, N-Triples loader, event schema |
| `shapestream/prepare_snapshots.py` | Raw dumps to snapshot tables (Parquet), with consistent subject sampling |
| `shapestream/make_stream.py` | Snapshots to change streams (one epoch per snapshot); `--mask-types` for Team A4 |
| `shapestream/gold_shapes.py` | Exact batch statistics per snapshot (the gold standard) |
| `shapestream/replay_to_kafka.py` | Replays a stream into Kafka with checkpoint markers |
| `shapestream/evaluate.py` | Compares your statistics with the gold and appends to the results table |
| `shapestream/to_shacl.py` | Statistics to SHACL shapes (Turtle) |
| `scripts/make_toy_kg.py` | Generates the toy KG (3 snapshots, about 7,000 triples each) |
| `scripts/download_dbpedia.sh` | Downloads the three DBpedia snapshots |
| `scripts/make_lubm.sh`, `scripts/rdfxml_to_nt.py` | Synthetic LUBM snapshots for scalability runs (optional) |
| `data/toy/` | Toy KG, already prepared: raw files, snapshots, streams `full` and `masked30`, gold |
| `results/` | Results tables (created by `evaluate.py`) |

## Quick start (toy KG, 5 minutes)

Inside the course container, in this folder:

```bash
# the toy data is already in data/toy; to rebuild it:
python scripts/make_toy_kg.py --out data/toy/raw
python -m shapestream.prepare_snapshots --out data/toy --snap 0 data/toy/raw/s0.nt --snap 1 data/toy/raw/s1.nt --snap 2 data/toy/raw/s2.nt
python -m shapestream.make_stream --data data/toy
python -m shapestream.make_stream --data data/toy --mask-types 0.3 --name masked30
python -m shapestream.gold_shapes --data data/toy
```
Then open `ProjectA_starter.ipynb`.

## Datasets

| Dataset | Use | Size | Source and license |
|---|---|---|---|
| Toy KG | development, unit tests | 3 snapshots, about 7,000 triples each | generated, `scripts/make_toy_kg.py` |
| DBpedia (English), releases 2022.03.01, 2022.09.01, 2022.12.01: `mappingbased-objects`, `mappingbased-literals`, `instance-types` (specific) | main experiments | about 0.4 GB compressed per release; use `--sample 0.05` on a laptop | [DBpedia Databus](https://databus.dbpedia.org/dbpedia/mappings/mappingbased-objects), CC BY 3.0. 2022.12.01 is the last public release |
| LUBM (UBA 1.7) | scalability (insert-only growth) | chosen by the number of universities | [Lehigh University](https://swat.cse.lehigh.edu/projects/lubm/) |

Each team builds the DBpedia data itself (step by step in `SETUP.md`):

```bash
bash scripts/download_dbpedia.sh data/dbpedia/raw
python -m shapestream.prepare_snapshots --out data/dbpedia --sample 0.05 --driver-memory 6g \
    --snap 0 "data/dbpedia/raw/2022.03.01/*" --snap 1 "data/dbpedia/raw/2022.09.01/*" --snap 2 "data/dbpedia/raw/2022.12.01/*"
python -m shapestream.make_stream --data data/dbpedia --driver-memory 6g
python -m shapestream.make_stream --data data/dbpedia --mask-types 0.3 --name masked30 --driver-memory 6g
python -m shapestream.gold_shapes --data data/dbpedia --driver-memory 6g
```

Sampling keeps every triple of a sampled subject (decided by a hash of the IRI, so the same subjects are in every
snapshot) and the `rdf:type` triples of the IRIs these subjects point to.

## Formats

**Event** (one JSON object per line in `streams/<name>/epoch=<k>/part-*.json`, and the Kafka message value):
```json
{"seq": 17, "epoch": 1, "op": "-", "s": "http://dbpedia.org/resource/Heraklion", "p": "http://dbpedia.org/ontology/populationTotal",
 "o": "140730", "okind": "literal", "dt": "http://www.w3.org/2001/XMLSchema#nonNegativeInteger", "lang": null}
```
`op` is `+` (insert) or `-` (delete). Epoch 0 loads snapshot 0; epoch k turns snapshot k-1 into snapshot k.
Within an epoch the events of a subject are consecutive and its deletions come first.

**Statistics** (what your engine writes at the end of each epoch, `runs/<team>/<dataset>/epoch<k>.json`): see
`shapestream/common.py`. In short, for every class C its size |C|; for every (C, p) the number of entities of C
with p and the observed min/max number of values; for every (C, p, T) the number of entities of C with a p-value
of type T (T = class of an IRI object, datatype of a literal, `urn:shapestream:IRI` for an untyped IRI).
Confidence = support / |C|. Entities without a type count under `urn:shapestream:Untyped`.

**Run info** (`run_info.json`, written by your engine): per epoch `latency_s`, `events_per_s`, `state_mb`.

## Evaluation

```bash
python -m shapestream.evaluate --gold data/dbpedia/gold/epoch1.json --pred runs/A1/dbpedia/epoch1.json \
    --run-info runs/A1/dbpedia/run_info.json --results results/results_A.csv
```
Constraint precision, recall and F1 at three threshold settings (support >= 1, 10, 50 with confidence >= 0, 0.1,
0.25), support MAPE, class-size MAPE and cardinality accuracy. Team A4 adds
`--groups runs/A4/dbpedia/groups_epoch1.jsonl --mask-truth data/dbpedia/streams/masked30/mask_truth/epoch=1`
for purity and adjusted Rand index of its groups. The mask truth is used only by `evaluate.py`; an engine that
reads it is disqualified (the instructor can rebuild the masked stream with another `--mask-seed` to check).

## Kafka

The course `docker-compose.yml` already runs Kafka (`kafka:9092` inside the containers, `localhost:29092` from
the host).
```bash
python -m shapestream.replay_to_kafka --stream data/toy/streams/full --topic kg-changes --rate 5000 --pause 30
```
Messages are keyed by subject, so a subject's events stay in order within one partition. A
`{"op": "checkpoint", "epoch": k}` message reaches every partition after the last event of epoch k.

## Related tools (baselines and background)

- QSE, batch shape extraction for large KGs: https://github.com/dkw-aau/qse (MIT; Java 17, N-Triples input)
- sheXer: `pip install shexer` (Apache-2.0), SHACL output with `output_format=SHACL_TURTLE`
- pySHACL to validate data against your shapes: `pip install pyshacl`
