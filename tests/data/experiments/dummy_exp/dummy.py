from __future__ import annotations

from typing import Any, Dict

from rem.core.experiment import ExperimentBase


class DemoExperiment(ExperimentBase):
    """
    Returns whatever params the system resolved (useful to assert overrides).
    """

    def run(self) -> Dict[str, Any]:
        # Convention: return payload contains params and simple metadata
        params = self.config.get("params", {})
        return {
            "status": "ok",
            "params": params,
        }
