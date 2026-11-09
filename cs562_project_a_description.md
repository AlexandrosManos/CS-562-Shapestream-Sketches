# CS-562 Advanced Topics in Databases
## Big Data Processing & Analytics

**Course Project A: ShapeStream**
*Incremental shape discovery over evolving knowledge graphs*

**Announced:** 2 November 2026  
**Presentations:** 14 and 16 December 2026  
**Report:** 15 January 2027, 23:59  
*Teams of 5. 4 teams on this project. Submission through eLearn.*

---

### Overview
Knowledge graphs such as DBpedia and Wikidata change all the time, while the SHACL shapes that describe them are extracted in batch from a snapshot and go stale. In this project, every team builds a Spark Structured Streaming engine that keeps the shapes of a KG up to date while triples are inserted and deleted. All four teams build the same exact engine first. Each team then replaces its state with one bounded-memory strategy and measures what it gains and what it loses. The four strategies together form one comparison, which we plan to write up as a research paper with the students whose results it uses.

**Background:** Lecture 3 (MinHash, LSH), Lecture 6 (sampling, DGIM, Bloom filters, HyperLogLog, Count-Min, Structured Streaming, Kafka), Lecture 7 (schema discovery, SHACL, PG-Schema), Lab 3 (Structured Streaming and Kafka), Lab 4 (schema discovery).

---

## 1. The problem

A KG is a set of triples $(s, p, o)$. For every class $C$ we want the shape of its instances:
*   $|C|$, the number of entities of class $C$ (entities without a type are counted under a special class `Untyped`);
*   for every property $p$: the support of $(C, p)$, that is the number of entities of $C$ that have $p$, and the observed minimum and maximum number of values (`sh:minCount`, `sh:maxCount`);
*   for every value type $T$ of $p$: the support of $(C, p, T)$, where $T$ is the class of an IRI object, the datatype of a literal, or IRI for an untyped object (`sh:class`, `sh:datatype`, `sh:nodeKind`).

The confidence of $(C, p, T)$ is $\frac{\text{support}}{|C|}$. A shape extractor keeps the constraints whose support and confidence pass two thresholds; the kit turns these statistics into SHACL (`shapestream/to_shacl.py`).

The input is a change stream: events that insert or delete one triple, grouped in epochs. Epoch $0$ loads snapshot $0$ of the KG, and epoch $k$ turns snapshot $k-1$ into snapshot $k$. At the end of every epoch your engine writes its statistics, and the kit compares them with the exact statistics of the snapshot (the gold standard).

Three things make the problem harder than counting:
*   **deletions:** a deleted triple can lower a maximum, or remove the last value of a property;
*   **type changes of a subject:** when an entity gets or loses a type, all its counts move between classes;
*   **type changes of an object:** when the object of a triple gets a new type, the value type $T$ of every triple that points to it changes, so constraints of other entities change.

## 2. Common part: the exact incremental engine (checkpoint 1, 20%)

Every team builds this engine first. It is the baseline of the paper, and every variant is compared with it.

1.  Read the stream with Spark Structured Streaming, from the epoch folders (`file` source, `trigger(availableNow=True)`) and from Kafka (`replay_to_kafka.py`). Process events in `seq` order within a subject.
2.  Keep the state needed to produce exact statistics after any micro-batch: entity types, per-entity property values (or counts), and what you need to handle the three cases above. The state for DBpedia does not fit in the driver; keep the large parts in Spark (state stores, or Parquet/Delta tables that you update per batch).
3.  At the end of every epoch write the statistics file (`shapestream.common.write_stats`) and `run_info.json` (latency, events per second, state size in MB).
4.  Do not recompute everything from all events at each epoch. Your update cost per epoch must depend on the number of changes in the epoch and on the entities they touch.

**Correctness test:** On the toy KG and on DBpedia (5% sample) your exact engine must reach $F1=1.0$ and support $\text{MAPE}=0$ at every epoch and every threshold. Report its latency, throughput and state size on DBpedia, and compare its time with re-running the batch extraction (`gold_shapes.py`) at each epoch.

## 3. Team variants (checkpoint 2 and final, 30%)

Each team replaces the exact state with one strategy, keeps everything else (input, output, evaluation) unchanged, and studies the trade-off between state size and accuracy. Parameters in brackets are the minimum grid to report.

**A1. Sketches**
Replace exact distinct counts ($|C|$, supports) with HyperLogLog and value frequencies with Count-Min sketches. Deletions: HLL does not support them; propose and compare two ways (for example, one HLL per epoch that is merged, or a Count-Min with conservative update over entity hashes).
*   **Grid:** HLL precision $p \in \{10, 12, 14\}$; Count-Min width in $\{2^{10}, 2^{12}, 2^{14}\}$, depth $4$.
*   **Report:** $F1$ and support MAPE against state size, at every epoch; how errors grow with deletions.

**A2. Sampling**
Keep a reservoir sample of entities per class (and a sample of objects for value types) and compute the statistics on the sample, scaled up. Handle deletions of sampled entities (for example, a backing sample).
*   **Grid:** reservoir size per class in $\{100, 1000, 10000\}$; with and without a minimum per class.
*   **Report:** $F1$, support MAPE and cardinality accuracy against state size; which classes and constraints are missed (rare classes, low-confidence constraints).

**A3. Windows**
Compute shapes over a sliding window of the last $W$ events (or the last $W$ epochs on DBpedia), with exponential buckets in the style of DGIM so that the state stays logarithmic in $W$.
Build a drift test: extend `make_toy_kg.py` so that a property is renamed or a class is split in epoch $k$, and generate a longer toy stream (10 or more epochs) with it.
*   **Grid:** $W \in \{1, 2, \text{all epochs on DBpedia}\}$; bucket error parameter in $\{0.5, 0.1\}$.
*   **Report:** how many epochs the shapes need to follow a drift, and the state size against the exact engine.

**A4. Type-free grouping**
Use the stream `masked30`, where 30% of the typed entities have lost their `rdf:type` triples. Group untyped entities on the fly with MinHash LSH over their property sets, and produce shapes per discovered group. Map groups to classes where possible, using the typed entities that fall in the same buckets.
*   **Grid:** number of hash functions in $\{64, 128\}$; bands $\times$ rows in $\{16 \times 4, 32 \times 4, 16 \times 8\}$; Jaccard threshold of the merge.
*   **Report:** purity and adjusted Rand index of the groups (`evaluate.py --groups --mask-truth`), $F1$ of the shapes on the masked stream compared with the exact engine on the same stream. Your engine must never read the mask truth; the instructor may rebuild the masked stream with another `--mask-seed` to check.

## 4. Data and kit

The kit is in `06_Projects/Project_A_ShapeStream/kit`. Each team sets everything up itself, following `SETUP.md` (about one afternoon): download DBpedia, build the snapshots, the streams and the gold, check Kafka, and commit the `SETUP_REPORT.txt` written by `scripts/check_setup.py`. Do not change the files of the `shapestream` package in your repository; write your engine in your own modules and notebooks.

| Dataset | Size | Use |
| :--- | :--- | :--- |
| **Toy KG** (3 snapshots) | about $7,000$ triples each | development, tests (in the kit, ready) |
| **DBpedia EN 2022.03 / 2022.09 / 2022.12**, 5% of subjects | a few million triples per snapshot | all experiments (built by the team, `SETUP.md`) |
| **DBpedia EN**, 20% of subjects | about $4$ times larger | optional scalability runs, if a machine of the team has the RAM |
| **DBpedia masked30 stream** | as above | Team A4 |
| **LUBM (UBA 1.7)** | chosen by number of universities | optional scalability runs (insert only) |

**Kit contents:** snapshot preparation with consistent sampling, stream builder, gold statistics, Kafka replayer with checkpoint markers, evaluation script, SHACL writer, starter notebook.

## 5. Experimental protocol

Every number goes into the shared results table through `python -m shapestream.evaluate --results results/results_A.csv`.

Report each measurement as the mean of 3 runs; give the machine (cores, RAM) and Spark settings. Report the exact engine on the same machine as your variant, so that the comparison is fair.
*   **Throughput and latency:** Kafka source, `replay_to_kafka.py --rate 0`; **accuracy:** `file` source is enough.
*   **Plots:** (a) $F1$ at $\sigma=10$, $\tau=0.1$ per epoch; (b) support MAPE against state size for your grid; (c) latency per epoch for the exact engine, your variant and batch re-extraction.

## 6. Milestones

| Date | Milestone | What we check |
| :--- | :--- | :--- |
| **Mon 2 Nov** | Kick-off (Project Discussion) | teams and variants fixed |
| **Mon 9 Nov & Mon 16 Nov** | Set-up done & Checkpoint 1 (15 min per team) | `SETUP_REPORT.txt` committed, all checks OK (DBpedia data built, Kafka works); exact engine: $F1=1.0$ on toy and DBpedia 5%, first timings |
| **Mon 30 Nov** | Checkpoint 2 (15 min per team) | variant runs end to end on DBpedia 5%; first grid points |
| **14 or 16 Dec** | Presentation (15 min + 5 min questions) | results of the full grid |
| **Fri 15 Jan 2027** | Report and code | 6-page report, repository tag `final` |

## 7. Deliverables and grading

| Part | Weight | Content |
| :--- | :--- | :--- |
| **Set-up and exact engine** (checkpoint 1) | 20% | set-up report on time; correctness test passed; code in the team repository |
| **Variant** | 30% | implementation, grid of experiments, rows in the results table |
| **Report** | 30% | 6 pages, LNCS format: problem, design of the exact engine, variant, experiments, discussion |
| **Presentation** | 20% | 15 minutes, all members present; one worked example of an epoch is required |

Each team is graded on its own engine and variant. A variant that does not reach its targets but is implemented, measured and explained honestly can still get full marks for the variant part.

## 8. Rules

*   One repository per team (GitHub or the CSD GitLab), shared with the instructor from day 1. Commit regularly; the history is part of the grade of each member.
*   Teams of this project may help each other with the set-up, the kit and the data. Code is written per team.
*   Use of AI assistants follows the course policy (Lecture 1): allowed, declared in the report, and every member must be able to explain every part of the code.
*   The results may be used in a research paper by the instructor. Students whose work is used will be invited as co-authors.

## 9. Reading

*   K. Rabbani, M. Lissandrini, K. Hose. *Extraction of Validating Shapes from very large Knowledge Graphs.* PVLDB 16(5), 2023 (QSE).
*   B. Dhouib, R. Appuswamy. *COSE: Continuous Shapes Extraction from Dynamic Knowledge Graphs.* IEEE BigData 2025.
*   E. Pürmayr. *SHACL Shapes Extraction for Evolving Knowledge Graphs.* Diploma thesis, TU Wien, 2025.
*   S. Ahmetaj, M. Ortiz, M. Šimkus. *Towards SHACL Validation of Evolving Graphs.* AMW 2024.
*   D. Fernández-Álvarez, J. E. Labra-Gayo, D. Gayo-Avello. *Automatic extraction of shapes using sheXer.* Knowledge-Based Systems, 2022.
*   P. Flajolet, É. Fusy, O. Gandouet, F. Meunier. *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm.* AofA 2007.
*   G. Cormode, S. Muthukrishnan. *An improved data stream summary: the Count-Min sketch and its applications.* J. Algorithms 55(1), 2005.
*   M. Datar, A. Gionis, P. Indyk, R. Motwani. *Maintaining stream statistics over sliding windows.* SIAM J. Computing 31(6), 2002.
*   J. S. Vitter. *Random sampling with a reservoir.* ACM TOMS 11(1), 1985.
*   A. Z. Broder. *On the resemblance and containment of documents.* SEQUENCES 1997.