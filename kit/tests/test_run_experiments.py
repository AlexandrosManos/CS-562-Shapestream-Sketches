from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


KIT_DIR = Path(__file__).resolve().parents[1]
if str(KIT_DIR) not in sys.path:
    sys.path.insert(0, str(KIT_DIR))

from scripts_team.run_experiments import (
    CM_WIDTH_VALUES,
    DATASETS,
    DELETION_STRATEGIES,
    HLL_VALUES,
    config_variant,
    parameter_grid,
    prediction_run_dir,
    run_evaluation,
)


class RunExperimentsTests(unittest.TestCase):
    def test_grid_includes_each_deletion_strategy_and_unique_variant_name(self):
        grid = list(parameter_grid())
        self.assertEqual(
            len(grid),
            1 + len(DELETION_STRATEGIES) * len(HLL_VALUES) * len(CM_WIDTH_VALUES),
        )
        variant_names = {
            config_variant(variant, strategy, hll, cm)
            for variant, strategy, hll, cm in grid
        }
        self.assertEqual(len(variant_names), len(grid))
        self.assertIn("A1-s1-p12-w4096", variant_names)

    def test_prediction_directories_are_unique_per_dataset_config_and_run(self):
        pred_root = Path("/predictions")
        prediction_dirs = [
            prediction_run_dir(
                pred_root,
                dataset,
                config_variant(variant, strategy, hll, cm),
                run,
            )
            for dataset in DATASETS
            for variant, strategy, hll, cm in parameter_grid()
            for run in range(1, 4)
        ]

        self.assertEqual(len(prediction_dirs), len(set(prediction_dirs)))
        self.assertEqual(
            prediction_run_dir(pred_root, "toy", "exact", 1),
            pred_root / "toy" / "exact" / "run_1",
        )

    def test_evaluation_uses_configured_variant_without_mutating_prediction(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            data_dir = root / "data"
            gold_dir = data_dir / "gold"
            pred_dir = root / "predictions"
            gold_dir.mkdir(parents=True)
            pred_dir.mkdir()
            (gold_dir / "epoch0.json").write_text("{}", encoding="utf-8")
            prediction_path = pred_dir / "epoch0.json"
            original_prediction = {"meta": {"variant": "A1"}}
            prediction_path.write_text(
                json.dumps(original_prediction), encoding="utf-8"
            )

            evaluated_prediction = {}

            def capture_evaluation(command, **_kwargs):
                pred_arg = command[command.index("--pred") + 1]
                evaluated_prediction.update(
                    json.loads(Path(pred_arg).read_text(encoding="utf-8"))
                )

            with patch(
                "scripts_team.run_experiments.subprocess.run",
                side_effect=capture_evaluation,
            ):
                run_evaluation(
                    str(data_dir),
                    str(pred_dir),
                    str(root / "results.csv"),
                    epochs=[0],
                    variant_name="A1-s1-p12-w4096",
                )

            self.assertEqual(
                evaluated_prediction["meta"]["variant"],
                "A1-s1-p12-w4096",
            )
            self.assertEqual(
                json.loads(prediction_path.read_text(encoding="utf-8")),
                original_prediction,
            )


if __name__ == "__main__":
    unittest.main()
