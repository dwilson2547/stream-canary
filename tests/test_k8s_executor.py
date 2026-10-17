import pytest

from stream_canary.executor import canary_deployment_manifest
from stream_canary.spec import RolloutSpec


def _k8s_spec() -> RolloutSpec:
    return RolloutSpec.model_validate(
        {
            "name": "unit-k8s",
            "prod": {"bootstrap": "b:9092", "input_topic": "in", "output_topic": "out"},
            "canary": {"bootstrap": "b:9092", "input_topic": "canary-in", "output_topic": "canary-out"},
            "deploy": {
                "runtime": "k8s",
                "k8s": {"namespace": "ns1", "image": "reg/svc:2.0.1", "name": "canary-1"},
                "env": {"BOOTSTRAP": "b:9092", "GROUP": "canary"},
                "consumer_group": "canary",
            },
        }
    )


def test_manifest_shape():
    m = canary_deployment_manifest(_k8s_spec())
    assert m["kind"] == "Deployment"
    assert m["metadata"]["name"] == "canary-1"
    assert m["metadata"]["namespace"] == "ns1"
    assert m["spec"]["template"]["spec"]["containers"][0]["image"] == "reg/svc:2.0.1"
    assert m["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"] == "Always"
    env = {e["name"]: e["value"] for e in m["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env == {"BOOTSTRAP": "b:9092", "GROUP": "canary"}


def test_manifest_labels_let_orchestrator_find_and_clean_canaries():
    m = canary_deployment_manifest(_k8s_spec())
    labels = m["metadata"]["labels"]
    assert labels["app.kubernetes.io/managed-by"] == "stream-canary"
    assert labels["stream-canary-rollout"] == "unit-k8s"
    # Selector must match template labels so the Deployment is valid.
    assert m["spec"]["selector"]["matchLabels"] == {"app": "canary-1"}
    assert m["spec"]["template"]["metadata"]["labels"]["app"] == "canary-1"


def test_manifest_requires_k8s_config():
    spec = _k8s_spec()
    spec.deploy.k8s = None
    with pytest.raises(ValueError):
        canary_deployment_manifest(spec)


def test_k8s_spec_requires_all_three_fields():
    base = _k8s_spec().model_dump()
    for missing in ("namespace", "image", "name"):
        bad = {k: v for k, v in base["deploy"]["k8s"].items() if k != missing}
        bad_spec = {**base, "deploy": {**base["deploy"], "k8s": bad}}
        with pytest.raises(Exception):
            RolloutSpec.model_validate(bad_spec)
