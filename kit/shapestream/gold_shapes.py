"""Exact batch statistics (the gold standard) for each snapshot.

    python -m shapestream.gold_shapes --data data/toy

Writes <data>/gold/epoch<k>.json in the statistics format of common.py.
The gold for epoch k is computed on snapshot k without masking, so it is also
the gold for masked streams.
"""
import argparse
import glob
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .common import BNODE, IRI_ANY, RDF_TYPE, UNTYPED, XSD_STRING, write_stats
from .spark_io import get_spark


def batch_stats(t: DataFrame):
    """Exact statistics of one triple table. Returns (classes, properties, constraints) dicts."""
    types = t.filter((F.col("p") == RDF_TYPE) & (F.col("okind") == "iri")) \
             .select("s", F.col("o").alias("cls")).distinct()
    subjects = t.select("s").distinct()
    subj_cls = subjects.join(types, "s", "left") \
                       .select("s", F.coalesce("cls", F.lit(UNTYPED)).alias("cls")).cache()
    classes = {r["cls"]: r["n"] for r in subj_cls.groupBy("cls").agg(F.count("*").alias("n")).collect()}

    data = t.filter(F.col("p") != RDF_TYPE)
    # values per (s, p); snapshot triples are distinct, so count(*) = number of distinct values
    per_sp = data.groupBy("s", "p").agg(F.count("*").alias("k"))
    props = per_sp.join(subj_cls, "s").groupBy("cls", "p") \
                  .agg(F.count("*").alias("support"), F.max("k").alias("max_count"))
    properties = {}
    for r in props.collect():
        mn = 1 if r["support"] == classes[r["cls"]] else 0
        properties[(r["cls"], r["p"])] = (r["support"], mn, r["max_count"])

    obj_types = types.select(F.col("s").alias("o"), F.col("cls").alias("otype"))
    vt = data.join(obj_types, "o", "left").select(
        "s", "p",
        F.when(F.col("okind") == "literal", F.coalesce("dt", F.lit(XSD_STRING)))
         .when(F.col("okind") == "bnode", F.lit(BNODE))
         .otherwise(F.coalesce("otype", F.lit(IRI_ANY))).alias("vtype")).distinct()
    cons = vt.join(subj_cls, "s").groupBy("cls", "p", "vtype").agg(F.countDistinct("s").alias("n"))
    constraints = {(r["cls"], r["p"], r["vtype"]): r["n"] for r in cons.collect()}
    subj_cls.unpersist()
    return classes, properties, constraints


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--driver-memory", default=None)
    a = ap.parse_args()
    spark = get_spark("gold_shapes", a.driver_memory)
    os.makedirs(os.path.join(a.data, "gold"), exist_ok=True)
    for p in sorted(glob.glob(os.path.join(a.data, "snapshots", "snap=*")), key=lambda x: int(x.rsplit("=", 1)[1])):
        k = int(p.rsplit("=", 1)[1])
        c, pr, co = batch_stats(spark.read.parquet(p))
        out = os.path.join(a.data, "gold", f"epoch{k}.json")
        write_stats(out, {"team": "gold", "variant": "batch-exact", "epoch": k,
                          "dataset": os.path.basename(os.path.normpath(a.data))}, c, pr, co)
        print(f"epoch {k}: {len(c)} classes, {len(pr)} property shapes, {len(co)} constraints -> {out}")
    spark.stop()


if __name__ == "__main__":
    main()
