from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, cast

from ml_collections import ConfigDict


class ExperimentBase(ABC):
    """
    Abstract base class for all experiments.
    """

    def __init__(self, config: ConfigDict) -> None:
        self.config = config

    @property
    def params(self) -> dict[str, Any]:
        base = dict(cast(Mapping[str, Any], self.config.get("params", {})))
        # Safe iteration over top-level (typed)
        top_items: Iterable[tuple[str, Any]] = cast(
            Iterable[tuple[str, Any]], cast(Mapping[str, Any], self.config).items()
        )
        # Surface top-level scalar overrides that collide with params keys
        for k, v in top_items:
            # surface scalar top-level overrides that collide with params
            if k in base and not isinstance(v, (Mapping, ConfigDict)):
                base[k] = v
        return base

    @abstractmethod
    def run(self) -> dict[str, Any]:
        """
        Run the experiment.

        Returns:
            A dictionary containing results, metrics, paths to artifacts, or any other outputs.
        """
        pass
