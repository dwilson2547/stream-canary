"""End-to-end rollout tests against a real Kafka (testcontainers).

Covers the full state machine: deploy canary -> mirror -> verify -> gate ->
promote/rollback -> teardown, on both the green path (clean v2.0.1 release,
promoted) and the red path (v2.0.0 regression, rolled back with prod untouched).
"""

import json
import os
import subprocess
import sys
import time

import pytest
from testcontainers.community.kafka import KafkaContainer

from stream_canary.executor import ensure_topics
from stream_canary.spec import load_spec
from stream_canary.verify import _drain_topic

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSOR = os.path.join(PROJECT_ROOT, "examples", "processors", "upper", "processor.py")


@pytest.fixture(scope="module")
def kafka():
    with KafkaContainer(image="confluentinc/cp-kafka:7.6.1") as container:
        bootstrap = container.get_bootstrap_server()
        ensure_topics(bootstrap, ["orders.raw", "orders.enriched"])
        yield bootstrap


def _produce_orders(bootstrap: str, count: int = 60) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap})
    for i in range(count):
        items = [
            {"sku": f"SKU-{i}", "qty": 1 + (i % 3), "unit_cents": 100 + i}
            for _ in range(1 + (i % 8))  # some orders exceed 5 items
        ]
        order = {"order_id": f"ord-{i:04d}", "customer": f"cust-{i:03d}", "items": items}
        producer.produce(
            "orders.raw", key=order["order_id"].encode(), value=json.dumps(order).encode()
        )
    producer.flush(15.0)


def _start_processor(bootstrap: str, version: str, group: str, input_topic: str,
                     output_topic: str, role: str) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(
        BOOTSTRAP=bootstrap, INPUT_TOPIC=input_topic, OUTPUT_TOPIC=output_topic,
        GROUP=group, PROCESSOR_VERSION=version,
    )
    proc = subprocess.Popen(
        [sys.executable, PROCESSOR, "--role", role], env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    return proc


def _wait_for_prod_drained(bootstrap: str, timeout_s: float = 60.0) -> None:
    from stream_canary.verify import group_lag

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if group_lag(bootstrap, "upper-prod", "orders.raw", 0) == 0:
            return
        time.sleep(1.0)
    raise TimeoutError("prod processor did not drain orders.raw")


def _wait_for_output_count(bootstrap: str, topic: str, expected: int, timeout_s: float = 90.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        records = _drain_topic(bootstrap, topic, max_messages=expected + 10, timeout_s=2.0)
        if len(records) >= expected:
            return records
        time.sleep(1.0)
    raise TimeoutError(f"only {len(_drain_topic(bootstrap, topic))} records on {topic}, expected {expected}")


def _write_spec(tmp_path, bootstrap: str, canary_version: str, allow_version: str) -> str:
    import yaml

    spec = {
        "name": f"e2e-{canary_version}",
        "broker": "kafka",
        "prod": {
            "bootstrap": bootstrap, "input_topic": "orders.raw",
            "output_topic": "orders.enriched", "partition": 0,
        },
        "canary": {
            "bootstrap": bootstrap, "input_topic": "canary.orders.raw",
            "output_topic": "canary.orders.enriched",
        },
        "deploy": {
            "runtime": "local",
            "command": f"{sys.executable} {PROCESSOR} --role canary",
            "env": {
                "BOOTSTRAP": bootstrap,
                "INPUT_TOPIC": "canary.orders.raw",
                "OUTPUT_TOPIC": "canary.orders.enriched",
                "GROUP": "canary-upper",
                "PROCESSOR_VERSION": canary_version,
            },
            "consumer_group": "canary-upper",
            "readiness_timeout_s": 90,
        },
        "mirror": {"mode": "replay", "sample_size": 60},
        "verify": [
            {"type": "schema", "fields": {
                "order_id": "str", "total_cents": "int", "status": "str", "version": "str",
            }},
            {"type": "equivalence", "join_key": "order_id",
             "ignore_fields": ["processed_at_ms"],
             "allow_diff": {"version": allow_version}},
            {"type": "lag", "group": "canary-upper", "topic": "canary.orders.raw", "max_lag": 10},
        ],
        "gates": [{"type": "exec", "command": "true"}],
        "on_success": "promote",
        "promote_command": "true",
        "workdir_root": str(tmp_path / "rollouts"),
    }
    path = tmp_path / f"spec-{canary_version}.yaml"
    path.write_text(yaml.safe_dump(spec))
    return str(path)


def test_green_path_promotes(kafka, tmp_path):
    """v2.0.1 is clean: verify passes (declared version diff), rollout promotes."""
    from stream_canary.orchestrator import run_rollout

    _produce_orders(kafka, 60)
    prod_proc = _start_processor(
        kafka, "1.4.2", "upper-prod", "orders.raw", "orders.enriched", "prod"
    )
    try:
        _wait_for_output_count(kafka, "orders.enriched", 60)
        _wait_for_prod_drained(kafka)

        spec_path = _write_spec(tmp_path, kafka, "2.0.1", "2.0.1")
        spec = load_spec(spec_path)
        exit_code = run_rollout(spec)

        assert exit_code == 0
        report = json.loads(
            (tmp_path / "rollouts" / "e2e-2.0.1").glob("*/report.json")
            .__next__().read_text()
        )
        assert report["outcome"] == "promoted"
        assert report["mirrored"] == 60
        checks = {c["check_type"]: c["passed"] for c in report["checks"]}
        assert checks == {"schema": True, "equivalence": True, "lag": True}
        assert report["gates"][0]["passed"] is True
    finally:
        prod_proc.terminate()
        prod_proc.wait(timeout=15)


def test_red_path_rolls_back(kafka, tmp_path):
    """v2.0.0 has the off-by-one regression: equivalence fails, rollout rolls back,
    prod outputs are untouched."""
    from stream_canary.orchestrator import run_rollout

    _produce_orders(kafka, 60)
    prod_proc = _start_processor(
        kafka, "1.4.2", "upper-prod", "orders.raw", "orders.enriched", "prod"
    )
    try:
        _wait_for_output_count(kafka, "orders.enriched", 60)
        _wait_for_prod_drained(kafka)
        before = sorted(
            json.loads(m.value())["order_id"] for m in _drain_topic(kafka, "orders.enriched")
        )

        spec_path = _write_spec(tmp_path, kafka, "2.0.0", "2.0.0")
        spec = load_spec(spec_path)
        exit_code = run_rollout(spec)

        assert exit_code == 1
        run_dirs = sorted((tmp_path / "rollouts" / "e2e-2.0.0").glob("*/report.json"))
        report = json.loads(run_dirs[-1].read_text())
        assert report["outcome"] == "rolled_back"
        assert report["failed_phase"] == "verify"

        checks = {c["check_type"]: c for c in report["checks"]}
        assert checks["schema"]["passed"] is True
        assert checks["equivalence"]["passed"] is False
        assert checks["equivalence"]["failures"], "regression should produce diff samples"
        assert "total_cents" in checks["equivalence"]["failures"][0]

        # Prod output untouched by the failed rollout.
        after = sorted(
            json.loads(m.value())["order_id"] for m in _drain_topic(kafka, "orders.enriched")
        )
        assert after == before
    finally:
        prod_proc.terminate()
        prod_proc.wait(timeout=15)
