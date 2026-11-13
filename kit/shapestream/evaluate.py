"""Compare a team's statistics with the gold statistics of the same epoch.

    python -m shapestream.evaluate --gold data/toy/gold/epoch1.json \
        --pred runs/A1/toy/epoch1.json --run-info runs/A1/toy/run_info.json \
        --results results/results_A.csv

Metrics (for each threshold setting sigma/tau):
  constraint precision / recall / F1   on the set of (C, p, T) with support >= sigma
                                       and confidence >= tau; the team's set uses
                                       the team's own estimates
  support MAPE                         mean absolute percentage error of support on
                                       constraints present in both sets
  class count MAPE                     the same for |C|
  cardinality accuracy                 share of (C, p) in both where min_count and
                                       max_count are both right (max_count counts
                                       as right if the team reports the exact value)
Optional --groups for Team A4 (discovered groups of untyped entities): purity and
adjusted Rand index against the held-out classes of the masked entities.
A row per threshold setting is appended to --results (CSV).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter, defaultdict
from math import comb

from .common import read_stats

THRESHOLDS = [(1, 0.0), (10, 0.1), (50, 0.25)]


def thresholded(classes, constraints, sigma, tau):
    out = {}
    for (c, p, t), n in constraints.items():
        size = classes.get(c, 0)
        if n >= sigma and size > 0 and n / size >= tau:
            out[(c, p, t)] = n
    return out


def mape(pairs):
    errs = [abs(p - g) / g for g, p in pairs if g]
    return sum(errs) / len(errs) if errs else None


def evaluate(gold_path, pred_path):
    _, gc, gp, gk = read_stats(gold_path)
    meta, pc, pp, pk = read_stats(pred_path)
    rows = []
    for sigma, tau in THRESHOLDS:
        G = thresholded(gc, gk, sigma, tau)
        P = thresholded(pc, pk, sigma, tau)
        tp = len(G.keys() & P.keys())
        prec = tp / len(P) if P else 0.0
        rec = tp / len(G) if G else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        rows.append({
            "sigma": sigma, "tau": tau,
            "gold_constraints": len(G), "pred_constraints": len(P),
            "precision": round(prec, 4), "recall": round(rec, 4), "f1": round(f1, 4),
            "support_mape": _r(mape([(G[k], P[k]) for k in G.keys() & P.keys()])),
        })
    common = gp.keys() & pp.keys()
    card_ok = sum(1 for k in common if gp[k][1] == pp[k][1] and gp[k][2] == pp[k][2])
    extra = {
        "class_count_mape": _r(mape([(gc[c], pc[c]) for c in gc.keys() & pc.keys()])),
        "classes_found": f"{len(gc.keys() & pc.keys())}/{len(gc)}",
        "cardinality_accuracy": _r(card_ok / len(common)) if common else None,
    }
    return meta, rows, extra


def group_scores(groups_path, truth_dir):
    """groups file: JSON lines {"s": iri, "group": id}; truth: JSON lines {"s", "class"}."""
    truth = defaultdict(set)
    for fn in sorted(os.listdir(truth_dir)):
        if fn.endswith(".json"):
            with open(os.path.join(truth_dir, fn)) as f:
                for line in f:
                    r = json.loads(line)
                    truth[r["s"]].add(r["class"])
    pred = {}
    with open(groups_path) as f:
        for line in f:
            r = json.loads(line)
            pred[r["s"]] = r["group"]
    # most specific class = the rarest class of the entity
    freq = Counter(c for cs in truth.values() for c in cs)
    lab = {s: min(cs, key=lambda c: (freq[c], c)) for s, cs in truth.items()}
    ents = [s for s in lab if s in pred]
    if not ents:
        return {"masked_entities": len(lab), "grouped": 0}
    cont = Counter((pred[s], lab[s]) for s in ents)
    by_group = defaultdict(Counter)
    for (g, c), n in cont.items():
        by_group[g][c] += n
    purity = sum(max(c.values()) for c in by_group.values()) / len(ents)
    # adjusted Rand index
    n = len(ents)
    a = Counter(pred[s] for s in ents)
    b = Counter(lab[s] for s in ents)
    sum_ij = sum(comb(v, 2) for v in cont.values())
    sum_a = sum(comb(v, 2) for v in a.values())
    sum_b = sum(comb(v, 2) for v in b.values())
    exp = sum_a * sum_b / comb(n, 2) if n > 1 else 0
    mx = (sum_a + sum_b) / 2
    ari = (sum_ij - exp) / (mx - exp) if mx != exp else 1.0
    return {"masked_entities": len(lab), "grouped": len(ents), "groups": len(by_group),
            "purity": round(purity, 4), "ari": round(ari, 4)}


def _r(x):
    return None if x is None else round(x, 4)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold", required=True)
    ap.add_argument("--pred", required=True)
    ap.add_argument("--run-info", help="JSON with latency_s, events_per_s, state_mb (written by your engine)")
    ap.add_argument("--groups", help="Team A4: JSON lines {s, group}")
    ap.add_argument("--mask-truth", help="Team A4: mask_truth/epoch=<k> directory")
    ap.add_argument("--results", help="CSV file to append the rows to")
    a = ap.parse_args()

    meta, rows, extra = evaluate(a.gold, a.pred)
    info = {}
    if a.run_info:
        with open(a.run_info) as f:
            info = json.load(f)
        if str(meta.get("epoch")) in info:          # per-epoch run info
            info = info[str(meta.get("epoch"))]
    if a.groups and a.mask_truth:
        extra.update(group_scores(a.groups, a.mask_truth))

    print(json.dumps({"meta": meta, "extra": extra, "run_info": info}, indent=1))
    print(f"{'sigma':>5} {'tau':>5} {'gold':>7} {'pred':>7} {'P':>6} {'R':>6} {'F1':>6} {'MAPE':>7}")
    for r in rows:
        print(f"{r['sigma']:>5} {r['tau']:>5} {r['gold_constraints']:>7} {r['pred_constraints']:>7} "
              f"{r['precision']:>6} {r['recall']:>6} {r['f1']:>6} {str(r['support_mape']):>7}")

    if a.results:
        fields = ["team", "variant", "dataset", "epoch", "sigma", "tau", "gold_constraints", "pred_constraints",
                  "precision", "recall", "f1", "support_mape", "class_count_mape", "cardinality_accuracy",
                  "latency_s", "events_per_s", "state_mb", "purity", "ari"]
        new = not os.path.exists(a.results)
        os.makedirs(os.path.dirname(a.results) or ".", exist_ok=True)
        with open(a.results, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            if new:
                w.writeheader()
            for r in rows:
                w.writerow({**{k: meta.get(k) for k in ("team", "variant", "dataset", "epoch")},
                            **r, **extra, **info})
        print(f"appended {len(rows)} rows to {a.results}")


if __name__ == "__main__":
    main()
