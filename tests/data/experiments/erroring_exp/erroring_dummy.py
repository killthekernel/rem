from typing import Any

from rem.core.experiment import ExperimentBase


class ExplodingExperiment(ExperimentBase):
    def run(self) -> dict[str, Any]:
        raise RuntimeError("Intentional test error.")
        return {}
