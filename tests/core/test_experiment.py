from ml_collections import ConfigDict

from tests.data.dummy_experiment import DummyExperiment


def test_dummy_experiment() -> None:
    cfg = ConfigDict({"x": 3, "y": 4})
    exp = DummyExperiment(cfg)
    result = exp.run()

    assert isinstance(result, dict)
    assert result["result"] == 7
    assert result["success"] is True
    assert result["inputs"] == {"x": 3, "y": 4}
