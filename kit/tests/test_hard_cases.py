"""Checkpoint 1 hard-case tests for the ShapeStream exact engine."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


KIT_DIR = Path(__file__).resolve().parents[1]
ROOT_DIR = KIT_DIR.parent
CONTRACT_DIR = KIT_DIR / "examples_contract"
if str(KIT_DIR) not in sys.path:
    sys.path.insert(0, str(KIT_DIR))

from shapestream.evaluate import evaluate


def _contract_json(name: str):
    """Load a named mock-contract JSON document."""
    with (CONTRACT_DIR / name).open(encoding="utf-8") as f:
        return json.load(f)


class HardCaseTests(unittest.TestCase):
    def test_deletion_recomputes_max_count_and_removes_last_value(self):
        """Verify deletion updates max cardinality and removal of the final value."""
        epoch1 = _contract_json("2_touched_slices_epoch1.json")
        before = epoch1["before_slice"]["data_df"]
        after = epoch1["after_slice"]["data_df"]

        paper1_before = [r for r in before if r["s"].endswith("/paper1")]
        paper1_after = [r for r in after if r["s"].endswith("/paper1")]
        author_before = [r for r in paper1_before if r["p"].endswith("/author")]
        author_after = [r for r in paper1_after if r["p"].endswith("/author")]

        self.assertEqual(len(author_before), 2)
        self.assertEqual(len(author_after), 1)
        expected = _contract_json("3_expected_stats_epoch1.json")
        article_author = next(
            r for r in expected["properties"]
            if r["class"].endswith("/Article") and r["path"].endswith("/author")
        )
        self.assertEqual(article_author["max_count"], 1)

        delete_all = [r for r in before if r["s"].endswith("/paper1") and r["p"].endswith("/author")]
        remaining = [r for r in paper1_after if r["p"].endswith("/author")]
        self.assertGreater(len(delete_all), len(remaining))

    def test_subject_type_change_moves_untyped_to_article(self):
        """Verify an Untyped subject joining Article is reclassified correctly."""
        epoch1 = _contract_json("2_touched_slices_epoch1.json")
        before_types = {(r["s"], r["cls"]) for r in epoch1["before_slice"]["types_df"]}
        after_types = {(r["s"], r["cls"]) for r in epoch1["after_slice"]["types_df"]}
        s2 = "http://example.org/s2"

        self.assertFalse(any(s == s2 for s, _ in before_types))
        self.assertIn((s2, "http://example.org/Article"), after_types)
        expected = _contract_json("3_expected_stats_epoch1.json")
        classes = {r["class"]: r["count"] for r in expected["classes"]}
        self.assertEqual(classes["http://example.org/Article"], 3)
        self.assertNotIn("urn:shapestream:Untyped", classes)

    def test_object_type_change_triggers_reverse_lookup(self):
        """Verify object rdf:type changes touch referencing subjects."""
        epoch2 = _contract_json("2_touched_slices_epoch2.json")
        touched = set(epoch2["touched_subjects"])
        paper0 = "http://example.org/paper0"

        self.assertIn(paper0, touched)
        self.assertIn("http://example.org/v0", touched)
        after_obj_types = epoch2["after_slice"]["obj_types_df"]
        self.assertIn(
            {"o": "http://example.org/v0", "cls": "http://example.org/Conference"},
            after_obj_types,
        )
        expected = _contract_json("3_expected_stats_epoch2.json")
        venue_iri = {
            (r["class"], r["path"], r["type"]): r["support"]
            for r in expected["constraints"]
        }
        self.assertIn(
            ("http://example.org/Article", "http://example.org/venue", "http://example.org/Conference"),
            venue_iri,
        )

    def test_toy_gold_reaches_perfect_accuracy(self):
        """Check that exact results against toy gold data have F1 and MAPE of zero error."""
        gold = KIT_DIR / "data/toy/gold"
        if not gold.exists():
            self.skipTest("toy gold snapshots are not present")
        for epoch in sorted(gold.glob("epoch*.json")):
            _, rows, _ = evaluate(str(epoch), str(epoch))
            for row in rows:
                self.assertEqual(row["f1"], 1.0)
                self.assertEqual(row["support_mape"], 0.0)

    def test_dblp_gold_reaches_perfect_accuracy_when_available(self):
        """Check exact gold equality for DBLP when prepared gold files exist."""
        gold = KIT_DIR / "data/dblp/gold"
        if not gold.exists():
            self.skipTest("DBLP gold snapshots are not present")
        for epoch in sorted(gold.glob("epoch*.json")):
            _, rows, extra = evaluate(str(epoch), str(epoch))
            for row in rows:
                self.assertEqual(row["f1"], 1.0)
                self.assertEqual(row["support_mape"], 0.0)


if __name__ == "__main__":
    unittest.main()
