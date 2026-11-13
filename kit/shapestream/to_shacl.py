"""Write SHACL shapes (Turtle) from a statistics file.

    python -m shapestream.to_shacl --stats runs/A1/toy/epoch2.json --out shapes_epoch2.ttl \
        --min-support 10 --min-confidence 0.1

One sh:NodeShape per class (sh:targetClass), one sh:PropertyShape per (class, path)
that passes the thresholds, with sh:minCount / sh:maxCount and, when a path has
several value types, an sh:or list of sh:class / sh:datatype alternatives.
Each property shape carries its support and confidence as annotations
(ss:support, ss:confidence) so that the shapes can be compared and pruned later.
"""
import argparse
import re
from collections import defaultdict

from .common import BNODE, IRI_ANY, UNTYPED, read_stats

XSD_NS = "http://www.w3.org/2001/XMLSchema#"
RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"


def local(iri: str) -> str:
    m = re.split(r"[/#:]", iri.rstrip("/#"))
    name = re.sub(r"[^A-Za-z0-9_]", "_", m[-1]) or "x"
    return name


def value_constraint(t: str) -> str:
    if t == IRI_ANY:
        return "sh:nodeKind sh:IRI"
    if t == BNODE:
        return "sh:nodeKind sh:BlankNode"
    if t.startswith(XSD_NS) or t.startswith(RDF_NS + "langString") or t.startswith(RDF_NS + "HTML"):
        return f"sh:datatype <{t}>"
    if "/datatype/" in t:                       # DBpedia custom datatypes
        return f"sh:datatype <{t}>"
    return f"sh:class <{t}>"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stats", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-support", type=float, default=1)
    ap.add_argument("--min-confidence", type=float, default=0.0)
    ap.add_argument("--base", default="http://hy562.csd.uoc.gr/shapes/")
    a = ap.parse_args()

    _, classes, props, cons = read_stats(a.stats)
    by_cp = defaultdict(list)
    for (c, p, t), n in cons.items():
        size = classes.get(c, 0)
        if size and n >= a.min_support and n / size >= a.min_confidence:
            by_cp[(c, p)].append((t, n))

    out = [f"@prefix sh: <http://www.w3.org/ns/shacl#> .",
           f"@prefix xsd: <{XSD_NS}> .",
           f"@prefix ss: <{a.base}vocab#> .",
           f"@prefix shape: <{a.base}> .", ""]
    names = set()
    for c in sorted(classes):
        if c == UNTYPED:
            continue
        cps = sorted(p for (cc, p) in by_cp if cc == c)
        if not cps:
            continue
        cname = local(c)
        while cname in names:
            cname += "_"
        names.add(cname)
        out.append(f"shape:{cname}Shape a sh:NodeShape ;")
        out.append(f"    sh:targetClass <{c}> ;")
        out.append(f"    ss:instances {classes[c]} ;")
        prop_lines = []
        for p in cps:
            support, mn, mx = props.get((c, p), (None, 0, None))
            vts = sorted(by_cp[(c, p)], key=lambda x: -x[1])
            conf = (support or max(n for _, n in vts)) / classes[c]
            parts = [f"sh:path <{p}>"]
            if len(vts) == 1:
                parts.append(value_constraint(vts[0][0]))
            else:
                alts = " ".join(f"[ {value_constraint(t)} ]" for t, _ in vts)
                parts.append(f"sh:or ( {alts} )")
            if mn == 1:
                parts.append("sh:minCount 1")
            if mx == 1:
                parts.append("sh:maxCount 1")
            parts.append(f"ss:support {support if support is not None else max(n for _, n in vts)}")
            parts.append(f"ss:confidence {conf:.4f}")
            prop_lines.append("[ " + " ; ".join(parts) + " ]")
        out.append("    sh:property " + " ,\n        ".join(prop_lines) + " .")
        out.append("")
    with open(a.out, "w", encoding="utf-8") as f:
        f.write("\n".join(out))
    print(f"wrote {len(names)} node shapes, {len(by_cp)} property shapes -> {a.out}")


if __name__ == "__main__":
    main()
