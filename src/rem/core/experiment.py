from abc import ABC, abstractmethod
from typing import Any

from ml_collections import ConfigDict


class ExperimentBase(ABC):
    """
    Abstract base class for all experiments.
    """

    def __init__(self, config: ConfigDict) -> None:
        self.config = config

    @abstractmethod
    def run(self) -> dict[str, Any]:
        """
        Run the experiment.

        Returns:
            A dictionary containing results, metrics, paths to artifacts, or any other outputs.
        """
        pass
