"""Replay a change stream into Kafka, epoch by epoch.

    # inside the course container (Kafka at kafka:9092)
    python -m shapestream.replay_to_kafka --stream data/toy/streams/full --topic kg-changes
    # from the host
    python -m shapestream.replay_to_kafka --stream ... --bootstrap localhost:29092 --rate 5000

Messages: key = subject IRI (so all events of a subject go to the same partition
and stay in order), value = the event JSON. After the last event of an epoch a
{"op": "checkpoint", "epoch": k} message is sent to every partition: your engine
writes its statistics for epoch k once it has seen that marker on all partitions.
--rate limits events per second (0 = as fast as possible); --pause waits between
epochs so that you can measure each epoch separately.
"""
import argparse
import glob
import json
import os
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", required=True, help="e.g. data/toy/streams/full")
    ap.add_argument("--topic", default="kg-changes")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--partitions", type=int, default=3)
    ap.add_argument("--rate", type=float, default=0)
    ap.add_argument("--pause", type=float, default=0)
    ap.add_argument("--epochs", default=None, help="comma-separated subset, e.g. 0,1")
    a = ap.parse_args()

    admin = KafkaAdminClient(bootstrap_servers=a.bootstrap)
    try:
        admin.create_topics([NewTopic(a.topic, num_partitions=a.partitions, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    prod = KafkaProducer(bootstrap_servers=a.bootstrap, linger_ms=20, batch_size=1 << 16,
                         key_serializer=lambda k: k.encode(), value_serializer=lambda v: json.dumps(v).encode())
    parts = sorted(prod.partitions_for(a.topic))
    epochs = sorted(glob.glob(os.path.join(a.stream, "epoch=*")), key=lambda p: int(p.rsplit("=", 1)[1]))
    if a.epochs:
        keep = {int(x) for x in a.epochs.split(",")}
        epochs = [e for e in epochs if int(e.rsplit("=", 1)[1]) in keep]
    for ep in epochs:
        k = int(ep.rsplit("=", 1)[1])
        t0, n = time.time(), 0
        for fn in sorted(glob.glob(os.path.join(ep, "part-*.json"))):
            with open(fn) as f:
                for line in f:
                    e = json.loads(line)
                    prod.send(a.topic, key=e["s"], value=e)
                    n += 1
                    if a.rate and n % 100 == 0:
                        ahead = n / a.rate - (time.time() - t0)
                        if ahead > 0:
                            time.sleep(ahead)
        prod.flush()
        for p in parts:
            prod.send(a.topic, key="__checkpoint__", value={"op": "checkpoint", "epoch": k}, partition=p)
        prod.flush()
        print(f"epoch {k}: {n} events in {time.time() - t0:.1f}s")
        if a.pause:
            time.sleep(a.pause)
    prod.close()


if __name__ == "__main__":
    main()
