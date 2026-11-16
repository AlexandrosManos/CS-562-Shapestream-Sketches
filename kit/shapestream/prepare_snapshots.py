"""Turn raw KG dumps into snapshot tables (Parquet), one per version.

Example (toy KG, no sampling):
    python -m shapestream.prepare_snapshots --out data/toy \
        --snap 0 data/toy/raw/s0.nt --snap 1 data/toy/raw/s1.nt --snap 2 data/toy/raw/s2.nt

Example (dblp monthly releases, 1 % of the subjects, the same subjects in every snapshot):
    python -m shapestream.prepare_snapshots --out data/dblp --sample 0.01 --drop-bnodes --driver-memory 6g \
        --snap 0 data/dblp/raw/dblp-2026-09-01.nt.gz \
        --snap 1 data/dblp/raw/dblp-2026-10-03.nt.gz \
        --snap 2 data/dblp/raw/dblp-2026-11-01.nt.gz

Sampling keeps every triple of a sampled subject (the hash of the subject IRI
decides, so a subject is in or out in all snapshots), plus the rdf:type triples
of the IRIs these subjects point to, so that object types are known.
--drop-bnodes removes every triple whose subject or object is a blank node. Blank-node labels
are regenerated in every dump (dblp writes its authorship "signatures" as blank nodes), so
without this option the diff between two snapshots would contain all of them.
A .gz file is read by a single Spark task; each snapshot is parsed once into a temporary
Parquet copy (<out>/_parsed/), which is deleted at the end. Expect about 30 to 60 minutes per
dblp snapshot on a laptop.
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
    ap.add_argument("--drop-bnodes", action="store_true", help="drop triples with a blank node (see above)")
    ap.add_argument("--keep-parsed", action="store_true", help="keep the temporary Parquet copy")
    ap.add_argument("--driver-memory", default=None)
    a = ap.parse_args()

    spark = get_spark("prepare_snapshots", a.driver_memory)
    report = {}
    for k, paths in sorted(a.snap, key=lambda x: int(x[0])):
        parsed_dir = os.path.join(a.out, "_parsed", f"snap={int(k)}")
        if not os.path.exists(os.path.join(parsed_dir, "_SUCCESS")):
            print(f"snapshot {k}: parsing {paths} (one pass over the file) ...", flush=True)
            load_nt(spark, paths.split(",")).repartition(64).write.mode("overwrite").parquet(parsed_dir)
        allt = spark.read.parquet(parsed_dir)
        bad = allt.filter(F.col("s") == "").count()
        allt = allt.filter(F.col("s") != "")
        if a.drop_bnodes:
            allt = allt.filter(~F.col("s").startswith("_:") & (F.col("okind") != "bnode"))
        allt = allt.dropDuplicates()
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
        json.dump({"sample": a.sample, "drop_bnodes": a.drop_bnodes, "snapshots": report}, f, indent=1)
    if not a.keep_parsed:
        import shutil
        shutil.rmtree(os.path.join(a.out, "_parsed"), ignore_errors=True)
    spark.stop()


if __name__ == "__main__":
    main()
