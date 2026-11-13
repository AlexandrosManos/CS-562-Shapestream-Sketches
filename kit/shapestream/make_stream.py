"""Build the change streams (one epoch per snapshot) from the snapshot tables.

    python -m shapestream.make_stream --data data/toy
    python -m shapestream.make_stream --data data/toy --mask-types 0.3 --name masked30

Epoch 0 inserts every triple of snapshot 0. Epoch k (k >= 1) contains the
difference from snapshot k-1 to snapshot k: deletions ("-") and insertions ("+").
Within an epoch, events are grouped by subject and a subject's deletions come
before its insertions; subjects appear in a fixed pseudo-random order.

Output: <data>/streams/<name>/epoch=<k>/part-*.json  (one JSON event per line)
        <data>/streams/<name>/manifest.json
With --mask-types F, the rdf:type triples of a fraction F of the typed subjects
are removed from every epoch (Team A4 setting). The true classes of the masked
subjects go to <data>/streams/<name>/mask_truth/epoch=<k>/; they are used only by
evaluate.py, and the engine must never read them.
"""
import argparse
import glob
import json
import os

from pyspark.sql import Window
from pyspark.sql import functions as F

from .common import RDF_TYPE
from .spark_io import get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--name", default="full")
    ap.add_argument("--mask-types", type=float, default=0.0)
    ap.add_argument("--mask-seed", default="mask",
                    help="a different seed masks a different set of entities")
    ap.add_argument("--driver-memory", default=None)
    a = ap.parse_args()

    spark = get_spark("make_stream", a.driver_memory)
    snap_dirs = sorted(glob.glob(os.path.join(a.data, "snapshots", "snap=*")),
                       key=lambda p: int(p.rsplit("=", 1)[1]))
    snaps = [spark.read.parquet(p) for p in snap_dirs]
    out_root = os.path.join(a.data, "streams", a.name)

    masked = None
    if a.mask_types > 0:
        typed = None
        for s in snaps:
            t = s.filter(F.col("p") == RDF_TYPE).select("s").distinct()
            typed = t if typed is None else typed.union(t).distinct()
        masked = typed.filter(F.pmod(F.xxhash64(F.concat(F.col("s"), F.lit("#" + a.mask_seed))), F.lit(1_000_000))
                              < int(a.mask_types * 1_000_000)).cache()

    def apply_mask(df, k):
        if masked is None:
            return df
        types = df.filter(F.col("p") == RDF_TYPE).join(masked, "s", "left_semi")
        types.select("s", F.col("o").alias("class")).write.mode("overwrite") \
             .json(os.path.join(out_root, "mask_truth", f"epoch={k}"))
        return df.join(types, ["s", "p", "o", "okind"], "left_anti")

    seq0 = 0
    manifest = {"name": a.name, "mask_types": a.mask_types, "epochs": []}
    prev = None
    for k, cur in enumerate(snaps):
        cur = apply_mask(cur, k)
        cols = ["s", "p", "o", "okind", "dt", "lang"]
        if prev is None:
            ev = cur.select(*cols).withColumn("op", F.lit("+"))
        else:
            dele = prev.select(*cols).exceptAll(cur.select(*cols)).withColumn("op", F.lit("-"))
            ins = cur.select(*cols).exceptAll(prev.select(*cols)).withColumn("op", F.lit("+"))
            ev = dele.unionByName(ins)
        ev = ev.withColumn("_h", F.pmod(F.xxhash64("s", F.lit(k)), F.lit(1 << 30))) \
               .withColumn("_o", F.when(F.col("op") == "-", 0).otherwise(1))
        w = Window.orderBy("_h", "s", "_o", "p", "o")
        ev = ev.withColumn("seq", F.row_number().over(w) + F.lit(seq0) - 1) \
               .withColumn("epoch", F.lit(k)) \
               .select("seq", "epoch", "op", *cols).orderBy("seq")
        out = os.path.join(out_root, f"epoch={k}")
        ev.coalesce(1).write.mode("overwrite").json(out)
        n = spark.read.json(out).count()
        n_del = ev.filter(F.col("op") == "-").count()
        manifest["epochs"].append({"epoch": k, "events": n, "deletions": n_del, "insertions": n - n_del,
                                   "first_seq": seq0, "last_seq": seq0 + n - 1})
        print(f"epoch {k}: {n} events ({n_del} deletions) -> {out}")
        seq0 += n
        prev = cur
    with open(os.path.join(out_root, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1)
    spark.stop()


if __name__ == "__main__":
    main()
