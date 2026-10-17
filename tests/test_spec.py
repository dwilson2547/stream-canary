import pytest
from pydantic import ValidationError

from stream_canary.spec import CanaryDeploy, RolloutSpec, load_spec


def _minimal_spec_dict(**overrides):
    spec = {
        "name": "test-rollout",
        "prod": {
            "bootstrap": "localhost:9092",
            "input_topic": "in",
            "output_topic": "out",
            "partition": 0,
        },
        "canary": {
            "bootstrap": "localhost:9092",
            "input_topic": "canary-in",
            "output_topic": "canary-out",
        },
        "deploy": {
            "runtime": "local",
            "command": "python canary.py",
            "consumer_group": "canary",
        },
        "verify": [],
        "gates": [],
    }
    spec.update(overrides)
    return spec


def test_minimal_spec_loads():
    spec = RolloutSpec.model_validate(_minimal_spec_dict())
    assert spec.mirror.mode == "replay"
    assert spec.mirror.sample_size == 100
    assert spec.on_success == "promote"


def test_local_runtime_requires_command():
    bad = _minimal_spec_dict()
    bad["deploy"] = {"runtime": "local", "consumer_group": "canary"}
    with pytest.raises(ValidationError):
        RolloutSpec.model_validate(bad)


def test_k8s_runtime_requires_k8s_config():
    bad = _minimal_spec_dict()
    bad["deploy"] = {"runtime": "k8s"}
    with pytest.raises(ValidationError):
        RolloutSpec.model_validate(bad)


def test_check_discriminators():
    spec_dict = _minimal_spec_dict()
    spec_dict["verify"] = [
        {"type": "schema", "fields": {"id": "str"}},
        {"type": "equivalence", "join_key": "id"},
        {"type": "lag", "group": "canary", "topic": "canary-in", "max_lag": 5},
    ]
    spec = RolloutSpec.model_validate(spec_dict)
    types = [c.type for c in spec.verify]
    assert types == ["schema", "equivalence", "lag"]


def test_schema_check_rejects_unknown_type():
    spec_dict = _minimal_spec_dict()
    spec_dict["verify"] = [{"type": "schema", "fields": {"id": "string"}}]
    with pytest.raises(ValidationError):
        RolloutSpec.model_validate(spec_dict)


def test_load_spec_from_yaml(tmp_path):
    import yaml

    spec_dict = _minimal_spec_dict()
    spec_dict["verify"] = [{"type": "equivalence", "join_key": "id", "allow_diff": {"version": "any"}}]
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(spec_dict))
    spec = load_spec(str(path))
    assert spec.verify[0].allow_diff == {"version": "any"}
