"""Shared definitions for the ShapeStream kit (HY562 Fall 2026, Project A).

Everything that two teams must agree on lives here: IRIs of special values,
the N-Triples parser, the event format and the statistics (shape) format.
Do not change this file in your team repository; the evaluation uses it as is.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Optional

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDF_LANGSTRING = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"

# Sentinel values used in the statistics format
UNTYPED = "urn:shapestream:Untyped"   # class of a subject that has no rdf:type
IRI_ANY = "urn:shapestream:IRI"       # value type of an IRI object that has no rdf:type
BNODE = "urn:shapestream:BNode"       # value type of a blank-node object

# --------------------------------------------------------------------------
# N-Triples parsing. DBpedia Databus .ttl files have one triple per line with
# full IRIs, so the same regular expression works for them.
# --------------------------------------------------------------------------
NT_REGEX = r'^\s*(<[^>]*>|_:\S+)\s+<([^>]*)>\s+(.*?)\s*\.\s*$'
OBJ_IRI = r'^<([^>]*)>$'
OBJ_BNODE = r'^(_:\S+)$'
OBJ_LIT = r'^"((?:[^"\\]|\\.)*)"(?:@([A-Za-z0-9-]+)|\^\^<([^>]*)>)?$'

_nt = re.compile(NT_REGEX)
_iri = re.compile(OBJ_IRI)
_bn = re.compile(OBJ_BNODE)
_lit = re.compile(OBJ_LIT)


@dataclass(frozen=True)
class Triple:
    s: str
    p: str
    o: str
    okind: str            # "iri" | "bnode" | "literal"
    dt: Optional[str] = None
    lang: Optional[str] = None

    def to_dict(self) -> dict:
        return {"s": self.s, "p": self.p, "o": self.o, "okind": self.okind,
                "dt": self.dt, "lang": self.lang}


def strip_subject(tok: str) -> str:
    return tok[1:-1] if tok.startswith("<") else tok


def parse_nt_line(line: str) -> Optional[Triple]:
    """Parse one N-Triples line. Returns None for comments and malformed lines."""
    if not line or line.lstrip().startswith("#"):
        return None
    m = _nt.match(line)
    if not m:
        return None
    s, p, o = strip_subject(m.group(1)), m.group(2), m.group(3)
    if (mi := _iri.match(o)):
        return Triple(s, p, mi.group(1), "iri")
    if (mb := _bn.match(o)):
        return Triple(s, p, mb.group(1), "bnode")
    if (ml := _lit.match(o)):
        lang = ml.group(2)
        dt = ml.group(3) or (RDF_LANGSTRING if lang else XSD_STRING)
        return Triple(s, p, ml.group(1), "literal", dt, lang)
    return None


def triple_to_nt(t: dict) -> str:
    s = t["s"] if t["s"].startswith("_:") else f"<{t['s']}>"
    if t["okind"] == "iri":
        o = f"<{t['o']}>"
    elif t["okind"] == "bnode":
        o = t["o"]
    else:
        if t.get("lang"):
            o = f'"{t["o"]}"@{t["lang"]}'
        elif t.get("dt") and t["dt"] != XSD_STRING:
            o = f'"{t["o"]}"^^<{t["dt"]}>'
        else:
            o = f'"{t["o"]}"'
    return f"{s} <{t['p']}> {o} ."


# --------------------------------------------------------------------------
# Event format (one JSON object per line, also the Kafka message value)
#   {"seq": 17, "epoch": 1, "op": "+", "s": ..., "p": ..., "o": ...,
#    "okind": "iri", "dt": null, "lang": null}
#   {"seq": 18, "epoch": 1, "op": "checkpoint"}      end of an epoch
# op "+" inserts the triple, op "-" deletes it.
# --------------------------------------------------------------------------
EVENT_FIELDS = ["seq", "epoch", "op", "s", "p", "o", "okind", "dt", "lang"]


def value_type(okind: str, o: str, dt: Optional[str], object_classes) -> list[str]:
    """Value types of one object, as used in constraints (C, p, T).

    object_classes: callable o -> set of classes (empty set if untyped).
    """
    if okind == "literal":
        return [dt or XSD_STRING]
    if okind == "bnode":
        return [BNODE]
    cls = object_classes(o)
    return sorted(cls) if cls else [IRI_ANY]


# --------------------------------------------------------------------------
# Statistics (shape) format: what every engine writes at each checkpoint.
#
# {
#   "meta": {"team": "A1", "variant": "exact", "epoch": 1, "dataset": "toy"},
#   "classes":   [{"class": C, "count": n}],                       |C|
#   "properties":[{"class": C, "path": p, "support": n,             entities of C with p
#                  "min_count": 0|1, "max_count": m}],              cardinality bounds observed
#   "constraints":[{"class": C, "path": p, "type": T, "support": n}] entities of C with a p-value of type T
# }
# Confidence of a constraint = support / |C|.
# A subject is counted under every class it has; subjects without a type count
# under UNTYPED. Approximate engines may write float supports.
# --------------------------------------------------------------------------

def write_stats(path: str, meta: dict, classes: dict, properties: dict, constraints: dict) -> None:
    """classes: {C: n}; properties: {(C,p): (support, min_count, max_count)};
    constraints: {(C,p,T): support}."""
    doc = {
        "meta": meta,
        "classes": [{"class": c, "count": n} for c, n in sorted(classes.items())],
        "properties": [{"class": c, "path": p, "support": v[0], "min_count": v[1], "max_count": v[2]}
                       for (c, p), v in sorted(properties.items())],
        "constraints": [{"class": c, "path": p, "type": t, "support": n}
                        for (c, p, t), n in sorted(constraints.items())],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1)


def read_stats(path: str):
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    classes = {r["class"]: r["count"] for r in doc["classes"]}
    properties = {(r["class"], r["path"]): (r["support"], r.get("min_count"), r.get("max_count"))
                  for r in doc["properties"]}
    constraints = {(r["class"], r["path"], r["type"]): r["support"] for r in doc["constraints"]}
    return doc.get("meta", {}), classes, properties, constraints
