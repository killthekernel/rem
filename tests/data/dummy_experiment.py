from typing import Any, cast

from ml_collections import ConfigDict

from rem.core.experiment import ExperimentBase


class DummyExperiment(ExperimentBase):
    def run(self) -> dict[str, Any]:
        x = cast(int, self.config.get("x", 1))
        y = cast(int, self.config.get("y", 2))
        return {
            "result": x + y,
            "inputs": {"x": x, "y": y},
            "success": True,
        }
