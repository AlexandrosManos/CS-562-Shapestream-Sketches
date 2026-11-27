"""Exact incremental statistics aggregator (Member 3 — Checkpoint 1)."""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

_KIT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _KIT_DIR not in sys.path:
    sys.path.insert(0, _KIT_DIR)

from engine.contracts import (
    DATA_SCHEMA,
    OBJ_TYPES_SCHEMA,
    TYPES_SCHEMA,
    ClassesDict,
    ConstraintsDict,
    StatsTuple,
    TouchedSlice,
)


@dataclass
class SliceCounts:
    """Compact grouped counts extracted from a single TouchedSlice."""
    # {class_name: entity_count}
    classes: Dict[str, int] = field(default_factory=dict)
    # {(class_name, prop_name): {k_values: entity_count}}
    prop_hists: Dict[Tuple[str, str], Dict[int, int]] = field(default_factory=dict)
    # {(class_name, prop_name, value_type): entity_count}
    constraints: Dict[Tuple[str, str, str], int] = field(default_factory=dict)


class ExactAggregator:
    """Maintains exact (classes, properties, constraints) via Before/After slice deltas."""

    def __init__(self) -> None:
        """Initialize empty class counts, (C, p) multiplicity histograms, and constraints."""
        self.classes: ClassesDict = {}
        self.prop_hists: Dict[Tuple[str, str], Dict[int, int]] = {}
        self.constraints: ConstraintsDict = {}

    def update(
        self,
        before_slice: Optional[TouchedSlice],
        after_slice: Optional[TouchedSlice],
    ) -> None:
        """Atomically subtract before_slice counts and add after_slice counts."""
        summary_before = self._summarize_slice(before_slice) if before_slice is not None else None
        summary_after = self._summarize_slice(after_slice) if after_slice is not None else None

        if summary_before is not None:
            self._validate_subtraction(summary_before)
            self._apply_counts(summary_before, -1)

        if summary_after is not None:
            self._apply_counts(summary_after, +1)

    def _validate_subtraction(self, counts: SliceCounts) -> None:
        """Ensure subtracting `counts` will not make any existing entry negative."""
        for key, value in counts.classes.items():
            if self.classes.get(key, 0) < value:
                raise ValueError(f"Negative count for class: {key}")
        for key, value in counts.prop_hists.items():
            hist = self.prop_hists.get(key, {})
            for k_values, entity_count in value.items():
                if hist.get(k_values, 0) < entity_count:
                    raise ValueError(f"Negative count for property {key} values {k_values}")
        for key, value in counts.constraints.items():
            if self.constraints.get(key, 0) < value:
                raise ValueError(f"Negative count for constraint: {key}")

    def _summarize_slice(self, slice_data: TouchedSlice) -> SliceCounts:
        """Aggregate a TouchedSlice in Spark into compact SliceCounts (handles Untyped & IRI)."""
        # Imported lazily so Spark-free unit checks work locally without PySpark
        from pyspark.sql import functions as F
        from shapestream.common import BNODE, IRI_ANY, UNTYPED, XSD_STRING

        types = slice_data.types_df.select("s", "cls").distinct()
        data = slice_data.data_df.select("s", "p", "o", "okind", "dt", "lang").distinct().cache()
        subjects = types.select("s").union(data.select("s")).distinct()
        subj_cls = (
            subjects.join(types, "s", "left")
            .select("s", F.coalesce("cls", F.lit(UNTYPED)).alias("cls"))
            .cache()
        )
        try:
            classes = {
                r["cls"]: int(r["n"])
                for r in subj_cls.groupBy("cls").agg(F.count("*").alias("n")).collect()
            }
            if not classes:
                return SliceCounts()

            per_sp = data.groupBy("s", "p").agg(F.count("*").alias("k"))
            hist_rows = (
                per_sp.join(subj_cls, "s")
                .groupBy("cls", "p", "k")
                .agg(F.count("*").alias("cnt"))
                .collect()
            )
            prop_hists: Dict[Tuple[str, str], Dict[int, int]] = {}
            for r in hist_rows:
                hist = prop_hists.setdefault((r["cls"], r["p"]), {})
                hist[int(r["k"])] = int(r["cnt"])

            obj_types = slice_data.obj_types_df.select("o", F.col("cls").alias("otype")).distinct()
            vt = (
                data.join(obj_types, "o", "left")
                .select(
                    "s",
                    "p",
                    F.when(F.col("okind") == "literal", F.coalesce("dt", F.lit(XSD_STRING)))
                    .when(F.col("okind") == "bnode", F.lit(BNODE))
                    .otherwise(F.coalesce("otype", F.lit(IRI_ANY)))
                    .alias("vtype"),
                )
                .distinct()
            )
            cons_rows = (
                vt.join(subj_cls, "s")
                .groupBy("cls", "p", "vtype")
                .agg(F.count("*").alias("n"))
                .collect()
            )
            constraints = {(r["cls"], r["p"], r["vtype"]): int(r["n"]) for r in cons_rows}
            return SliceCounts(classes=classes, prop_hists=prop_hists, constraints=constraints)
        finally:
            subj_cls.unpersist()
            data.unpersist()
        
    def _apply_counts(self, counts: SliceCounts, sign: int) -> None:
        """Apply signed (+1 or -1) SliceCounts to in-memory dicts and prune zeros."""
        if sign not in (1, -1):
            raise ValueError("sign must be 1 or -1")

        def _apply_dict(count_dict: dict, agg_dict: dict, label: str) -> None:
            for key, value in count_dict.items():
                count = agg_dict.get(key, 0) + sign * value
                if count == 0:
                    agg_dict.pop(key, None)
                elif count > 0:
                    agg_dict[key] = count
                else:
                    raise ValueError(f"Negative count for {label}: {key}")

        _apply_dict(counts.classes, self.classes, "class")

        for key, value in counts.prop_hists.items():
            hist = self.prop_hists.setdefault(key, {})
            _apply_dict(value, hist, f"property {key}")
            if not hist:
                del self.prop_hists[key]
        
        _apply_dict(counts.constraints, self.constraints, "constraint")
    
    def current_stats(self) -> StatsTuple:
        """Return (classes, properties, constraints) formatted for write_stats()."""        
        classes = dict(self.classes)
        props = {}
        for (cls, prop), hist in self.prop_hists.items():
            support = sum(hist.values())
            min_count = 1 if support == classes[cls] else 0
            max_count = max(hist.keys())
            props[(cls, prop)] = (support, min_count, max_count)
        
        constraints = dict(self.constraints)

        return classes, props, constraints

    def state_bytes(self) -> int:
        """Return estimated memory size in bytes of the in-memory summary dicts."""
        def _sizeof(obj: object) -> int:
            size = sys.getsizeof(obj)
            if isinstance(obj, dict):
                return size + sum(_sizeof(k) + _sizeof(v) for k, v in obj.items())
            if isinstance(obj, tuple):
                return size + sum(_sizeof(item) for item in obj)
            return size

        return _sizeof(self.classes) + _sizeof(self.prop_hists) + _sizeof(self.constraints)


def run_contract_demo() -> None:
    """Run ExactAggregator on examples_contract slices (Epochs 0-2) and verify expected stats."""
    import json

    from shapestream.common import read_stats
    from shapestream.spark_io import get_spark

    examples_dir = os.path.join(_KIT_DIR, "examples_contract")
    spark = get_spark("exact_agg_demo")

    def _to_slice(d: dict) -> TouchedSlice:
        return TouchedSlice(
            types_df=spark.createDataFrame(d["types_df"], schema=TYPES_SCHEMA),
            data_df=spark.createDataFrame(d["data_df"], schema=DATA_SCHEMA),
            obj_types_df=spark.createDataFrame(d["obj_types_df"], schema=OBJ_TYPES_SCHEMA),
        )

    agg = ExactAggregator()
    try:
        for epoch in (0, 1, 2):
            slice_path = os.path.join(examples_dir, f"2_touched_slices_epoch{epoch}.json")
            exp_path = os.path.join(examples_dir, f"3_expected_stats_epoch{epoch}.json")
            with open(slice_path, encoding="utf-8") as f:
                doc = json.load(f)

            agg.update(_to_slice(doc["before_slice"]), _to_slice(doc["after_slice"]))
            classes, props, cons = agg.current_stats()
            _, exp_classes, exp_props, exp_cons = read_stats(exp_path)

            assert classes == exp_classes, f"Epoch {epoch} classes mismatch: {classes} != {exp_classes}"
            assert props == exp_props, f"Epoch {epoch} properties mismatch: {props} != {exp_props}"
            assert cons == exp_cons, f"Epoch {epoch} constraints mismatch: {cons} != {exp_cons}"
            print(
                f"[OK] Epoch {epoch}: {len(classes)} classes, {len(props)} properties, "
                f"{len(cons)} constraints (driver state={agg.state_bytes()} B)"
            )
        print("ALL 3 MOCK EPOCHS PASSED!")
    finally:
        spark.stop()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments when exact_agg.py is executed directly."""
    parser = argparse.ArgumentParser(description="Exact incremental shape aggregator.")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run verification against examples_contract mock data.",
    )
    return parser.parse_args(argv)


def test_apply_counts() -> None:
    """Fast Spark-free unit check for _apply_counts, current_stats, and state_bytes."""
    agg = ExactAggregator()

    c0 = SliceCounts(
        classes={"Article": 2, "Untyped": 1},
        prop_hists={
            ("Article", "author"): {1: 1, 2: 1},
            ("Article", "venue"): {1: 1},
            ("Untyped", "title"): {1: 1},
        },
        constraints={
            ("Article", "author", "string"): 2,
            ("Article", "venue", "IRI"): 1,
            ("Untyped", "title", "langString"): 1,
        },
    )
    agg._apply_counts(c0, sign=+1)
    cls1, props1, cons1 = agg.current_stats()
    assert cls1 == {"Article": 2, "Untyped": 1}
    assert props1 == {
        ("Article", "author"): (2, 1, 2),
        ("Article", "venue"): (1, 0, 1),
        ("Untyped", "title"): (1, 1, 1),
    }
    assert cons1 == {
        ("Article", "author", "string"): 2,
        ("Article", "venue", "IRI"): 1,
        ("Untyped", "title", "langString"): 1,
    }

    c1 = SliceCounts(
        classes={"Article": 1, "Untyped": 1},
        prop_hists={
            ("Article", "author"): {2: 1},
            ("Untyped", "title"): {1: 1},
        },
        constraints={
            ("Article", "author", "string"): 1,
            ("Untyped", "title", "langString"): 1,
        },
    )
    agg._apply_counts(c1, sign=-1)
    cls2, props2, cons2 = agg.current_stats()
    assert cls2 == {"Article": 1}
    assert props2 == {("Article", "author"): (1, 1, 1), ("Article", "venue"): (1, 1, 1)}
    assert cons2 == {("Article", "author", "string"): 1, ("Article", "venue", "IRI"): 1}
    assert agg.state_bytes() > 0
    print(f"[OK] Local unit tests passed! (driver state={agg.state_bytes()} B)")


if __name__ == "__main__":
    args = parse_args()
    if args.demo:
        run_contract_demo()
    else:
        test_apply_counts()
