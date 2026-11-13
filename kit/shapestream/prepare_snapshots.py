"""Turn raw KG dumps into snapshot tables (Parquet), one per version.

Example (toy KG, no sampling):
    python -m shapestream.prepare_snapshots --out data/toy \
        --snap 0 data/toy/raw/s0.nt --snap 1 data/toy/raw/s1.nt --snap 2 data/toy/raw/s2.nt

Example (DBpedia, 5 % of the subjects, the same subjects in every snapshot):
    python -m shapestream.prepare_snapshots --out data/dbpedia --sample 0.05 \
        --snap 0 "data/dbpedia/raw/2022.03.01/*" \
        --snap 1 "data/dbpedia/raw/2022.09.01/*" \
        --snap 2 "data/dbpedia/raw/2022.12.01/*"

Sampling keeps every triple of a sampled subject (the hash of the subject IRI
decides, so a subject is in or out in all snapshots), plus the rdf:type triples
of the IRIs these subjects point to, so that object types are known.
Output: <out>/snapshots/snap=<k>/ (Parquet, triple schema) and <out>/snapshots.json.
"""
import argparse
import json
import os

from pyspark.sql import functions as F

from .common import RDF_TYPE
from .spark_io import get_spark, load_nt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--snap", nargs=2, action="append", metavar=("K", "PATHS"), required=True,
                    help="snapshot number and a comma-separated list of files or globs")
    ap.add_argument("--sample", type=float, default=1.0, help="fraction of subjects to keep (0-1]")
    ap.add_argument("--driver-memory", default=None)
    a = ap.parse_args()

    spark = get_spark("prepare_snapshots", a.driver_memory)
    report = {}
    for k, paths in sorted(a.snap, key=lambda x: int(x[0])):
        allt = load_nt(spark, paths.split(","))
        bad = allt.filter(F.col("s") == "").count()
        allt = allt.filter(F.col("s") != "").dropDuplicates()
        if a.sample < 1.0:
            keep = F.pmod(F.xxhash64("s"), F.lit(1_000_000)) < int(a.sample * 1_000_000)
            core = allt.filter(keep)
            refs = core.filter(F.col("okind") == "iri").select(F.col("o").alias("s")).distinct()
            obj_types = allt.filter(F.col("p") == RDF_TYPE).join(refs, "s", "left_semi")
            snap = core.unionByName(obj_types).dropDuplicates()
        else:
            snap = allt
        out = os.path.join(a.out, "snapshots", f"snap={int(k)}")
        snap.repartition(8, "s").write.mode("overwrite").parquet(out)
        n = spark.read.parquet(out).count()
        report[int(k)] = {"inputs": paths, "triples": n, "malformed_lines": bad}
        print(f"snapshot {k}: {n} triples, {bad} malformed lines skipped -> {out}")
    with open(os.path.join(a.out, "snapshots.json"), "w") as f:
        json.dump({"sample": a.sample, "snapshots": report}, f, indent=1)
    spark.stop()


if __name__ == "__main__":
    main()
