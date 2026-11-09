# shapestream-sketches

**CS-562 Advanced Topics in Databases (Project A, Variant A1)**
Incremental SHACL shape discovery over evolving knowledge graphs with Spark Structured Streaming, using bounded-memory sketches.

## Overview

Knowledge graphs such as DBpedia change all the time, so shapes extracted from a single snapshot go stale. This project builds a streaming engine that keeps the shape statistics of a KG up to date while triples are inserted and deleted:

- `|C|`: the number of entities of each class
- support and min/max cardinality of each `(C, p)`
- support of each value type `(C, p, T)` (`sh:class`, `sh:datatype`, `sh:nodeKind`)

It has two parts:

1. **Exact incremental engine** (baseline): it reaches F1 = 1.0 and support MAPE = 0 at every epoch, and the cost of each update depends only on the changes in that epoch.
2. **A1, sketch variant**: exact distinct counts are replaced with **HyperLogLog** and value frequencies with **Count-Min** sketches. We compare two ways of handling deletions:
   - one HLL per epoch, merged across epochs
   - Count-Min with conservative update over entity hashes

## Experiment grid

| Parameter | Values |
|---|---|
| HLL precision `p` | 10, 12, 14 |
| Count-Min width | 2^10, 2^12, 2^14 (depth 4) |

For every epoch we report F1 and support MAPE against state size, and how the errors grow with deletions.

## Data

- Toy KG (3 snapshots, about 7k triples each), used for development and tests
- DBpedia EN 2022.03 / 2022.09 / 2022.12, 5% subject sample, used for all experiments

See `SETUP.md` for how to build the data, and `SETUP_REPORT.txt` for the result of the setup check.

## Repository layout

```
-
```

## Running

```bash
-
```

## Team

| Name | GitHub |
|---|---|
| | |

