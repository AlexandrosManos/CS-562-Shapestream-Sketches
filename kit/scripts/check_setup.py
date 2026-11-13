"""Check that the Project A set-up is complete and write SETUP_REPORT.txt (commit it to your repository).

    python scripts/check_setup.py                 # toy data only
    python scripts/check_setup.py --data data/dbpedia
"""
import argparse
import glob
import json
import os
import platform
import socket
import sys

sys.path.insert(0, os.getcwd())
lines, ok_all = [], True


def check(name, ok, detail=""):
    global ok_all
    ok_all &= bool(ok)
    lines.append(f"[{'OK ' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""))


def data_set(path):
    man = os.path.join(path, "streams", "full", "manifest.json")
    check(f"{path}: snapshots", len(glob.glob(os.path.join(path, "snapshots", "snap=*"))) >= 3)
    if os.path.exists(man):
        m = json.load(open(man))
        check(f"{path}: stream 'full'", len(m["epochs"]) >= 3,
              ", ".join(f"epoch {e['epoch']}: {e['events']} events" for e in m["epochs"]))
    else:
        check(f"{path}: stream 'full'", False, "run shapestream.make_stream")
    check(f"{path}: stream 'masked30'", os.path.exists(os.path.join(path, "streams", "masked30", "manifest.json")))
    gold = sorted(glob.glob(os.path.join(path, "gold", "epoch*.json")))
    check(f"{path}: gold", len(gold) >= 3, f"{len(gold)} files")


ap = argparse.ArgumentParser()
ap.add_argument("--data", default=None)
ap.add_argument("--kafka", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
a = ap.parse_args()

lines.append(f"python {platform.python_version()} on {platform.platform()}")
try:
    import pyspark
    check("pyspark", True, pyspark.__version__)
    from shapestream.spark_io import get_spark
    spark = get_spark("check")
    check("Spark session", spark.range(10).count() == 10, f"cores={spark.sparkContext.defaultParallelism}")
    spark.stop()
except Exception as e:
    check("pyspark / Spark session", False, str(e)[:200])
host, _, port = a.kafka.partition(":")
try:
    socket.create_connection((host, int(port or 9092)), timeout=3).close()
    check("Kafka reachable", True, a.kafka)
except OSError as e:
    check("Kafka reachable", False, f"{a.kafka}: {e} (start the course docker-compose)")
data_set("data/toy")
if a.data:
    data_set(a.data)
lines.append("ALL CHECKS PASSED" if ok_all else "SOME CHECKS FAILED")
open("SETUP_REPORT.txt", "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
