"""Convert the RDF/XML (.owl) files written by the LUBM generator to one N-Triples file.

    python scripts/rdfxml_to_nt.py --out data/lubm/raw/s0.nt data/lubm/gen/u0 [more folders]
Needs rdflib (pip install rdflib).
"""
import argparse
import glob
import os

import rdflib

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("folders", nargs="+")
a = ap.parse_args()
n = 0
with open(a.out, "w", encoding="utf-8") as out:
    for d in a.folders:
        for fn in sorted(glob.glob(os.path.join(d, "*.owl"))):
            g = rdflib.Graph()
            g.parse(fn, format="xml")
            data = g.serialize(format="nt")
            out.write(data if data.endswith("\n") else data + "\n")
            n += len(g)
print(f"{n} triples -> {a.out}")
