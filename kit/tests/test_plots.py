from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


KIT_DIR = Path(__file__).resolve().parents[1]
if str(KIT_DIR) not in sys.path:
    sys.path.insert(0, str(KIT_DIR))

from scripts_team.plots import (
    _cumulative_deletions,
    _deletion_growth_averages,
    _tradeoff_averages,
)


class PlotTests(unittest.TestCase):
    def test_tradeoff_averages_filter_threshold_and_average_runs_per_epoch_variant(self):
        rows = [
            {
                "variant": "A1-s1-p10-w1024",
                "epoch": "0",
                "sigma": "10",
                "tau": "0.1",
                "state_mb": str(state),
                "support_mape": str(mape),
            }
            for state, mape in ((10, 0.3), (12, 0.5), (11, 0.4))
        ]
        rows.extend(
            [
                {
                    "variant": "A1-s1-p12-w4096",
                    "epoch": "0",
                    "sigma": "10",
                    "tau": "0.1",
                    "state_mb": "20",
                    "support_mape": "0.2",
                },
                {
                    "variant": "A1-s1-p10-w1024",
                    "epoch": "0",
                    "sigma": "50",
                    "tau": "0.25",
                    "state_mb": "100",
                    "support_mape": "99",
                },
                {
                    "variant": "A1-s1-p10-w1024",
                    "epoch": "1",
                    "sigma": "10",
                    "tau": "0.1",
                    "state_mb": "14",
                    "support_mape": "0.6",
                },
            ]
        )

        averages = _tradeoff_averages(rows)
        expected = {
            0: {
                "A1-s1-p10-w1024": (11.0, 0.4),
                "A1-s1-p12-w4096": (20.0, 0.2),
            },
            1: {"A1-s1-p10-w1024": (14.0, 0.6)},
        }
        self.assertEqual(averages.keys(), expected.keys())
        for epoch, expected_variants in expected.items():
            self.assertEqual(averages[epoch].keys(), expected_variants.keys())
            for variant, expected_point in expected_variants.items():
                for actual_value, expected_value in zip(
                    averages[epoch][variant], expected_point
                ):
                    self.assertAlmostEqual(actual_value, expected_value)

    def test_deletion_growth_averages_by_dataset_strategy_and_epoch(self):
        rows = [
            {
                "dataset": "toy",
                "variant": "A1-s1-p10-w1024",
                "epoch": "1",
                "sigma": "10",
                "tau": "0.1",
                "support_mape": "0.2",
            },
            {
                "dataset": "toy",
                "variant": "A1-s1-p12-w4096",
                "epoch": "1",
                "sigma": "10",
                "tau": "0.1",
                "support_mape": "0.4",
            },
            {
                "dataset": "toy",
                "variant": "A1-s2-p10-w1024",
                "epoch": "1",
                "sigma": "10",
                "tau": "0.1",
                "support_mape": "0.6",
            },
            {
                "dataset": "toy",
                "variant": "A1-s1-p10-w1024",
                "epoch": "1",
                "sigma": "50",
                "tau": "0.25",
                "support_mape": "99",
            },
            {
                "dataset": "dblp",
                "variant": "A1-s1-p10-w1024",
                "epoch": "1",
                "sigma": "10",
                "tau": "0.1",
                "support_mape": "0.8",
            },
        ]

        averages = _deletion_growth_averages(rows)

        self.assertAlmostEqual(averages[("toy", "s1")][1], 0.3)
        self.assertEqual(averages[("toy", "s2")][1], 0.6)
        self.assertEqual(averages[("dblp", "s1")][1], 0.8)

    def test_cumulative_deletions_come_from_epoch_manifest(self):
        manifest = {
            "epochs": [
                {"epoch": 2, "deletions": 202},
                {"epoch": 0, "deletions": 0},
                {"epoch": 1, "deletions": 199},
            ]
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")

            self.assertEqual(
                _cumulative_deletions(path),
                {0: 0, 1: 199, 2: 401},
            )


if __name__ == "__main__":
    unittest.main()
