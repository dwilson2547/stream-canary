"""Verify conditions: schema, equivalence, lag.

Every check returns a CheckResult; the verify phase fails on the first failing
check but records results for all checks it managed to run.
"""

import json
import logging
import time
import uuid
from dataclasses import dataclass, field

from confluent_kafka import Consumer, TopicPartition

from .journal import Journal
from .spec import EquivalenceCheck, LagCheck, SchemaCheck

log = logging.getLogger("stream_canary.verify")


@dataclass
class CheckResult:
    check_type: str
    passed: bool
    summary: str
    failures: list[str] = field(default_factory=list)
    # For equivalence: how many records were compared / missing on each side.
    compared: int = 0
    missing_canary: int = 0
    missing_prod: int = 0


def _drain_topic(bootstrap: str, topic: str, min_expected: int = 0,
                 max_wait_s: float = 60.0, timeout_s: float = 5.0,
                 max_messages: int = 100_000):
    """Read a topic without disturbing any consumer group.

    Completion rule: keep polling until at least `min_expected` records have
    been seen AND two consecutive windows come back empty (a single empty
    window can be a rebalance pause). If `min_expected` is never reached,
    give up after `max_wait_s` and return what was read — the caller decides
    whether the shortfall is a failure.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"stream-canary-verify-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    records = []
    started = time.monotonic()
    try:
        consumer.subscribe([topic])
        empty_streak = 0
        while len(records) < max_messages:
            if len(records) >= min_expected and empty_streak >= 2:
                break
            if len(records) < min_expected and time.monotonic() - started > max_wait_s:
                break
            batch = consumer.consume(num_messages=100, timeout=timeout_s)
            got_any = False
            for msg in batch:
                if msg.error():
                    continue
                got_any = True
                records.append(msg)
            empty_streak = 0 if got_any else empty_streak + 1
    finally:
        consumer.close()
    return records


def _payloads(records) -> list[dict]:
    out = []
    for msg in records:
        if msg.value() is None:
            continue
        try:
            out.append(json.loads(msg.value()))
        except json.JSONDecodeError as exc:
            out.append({"__unparseable__": msg.value().decode("utf-8", "replace"), "__error__": str(exc)})
    return out


def check_schema(bootstrap: str, topic: str, check: SchemaCheck,
                 min_expected: int = 0) -> CheckResult:
    records = _drain_topic(bootstrap, topic, min_expected=min_expected)
    payloads = _payloads(records)
    if not payloads:
        return CheckResult("schema", False, f"no messages on {topic} to validate")

    type_map = {"str": str, "int": int, "float": float, "bool": bool}
    failures = []
    for i, payload in enumerate(payloads):
        for name, expected in check.fields.items():
            if name not in payload:
                failures.append(f"message {i}: missing field {name!r}")
            elif isinstance(payload[name], bool) and expected == "int":
                failures.append(f"message {i}: field {name!r} is bool, expected int")
            elif not isinstance(payload[name], type_map[expected]):
                # float accepts ints, int does not accept floats.
                if expected == "float" and isinstance(payload[name], (int, float)):
                    continue
                failures.append(
                    f"message {i}: field {name!r} is {type(payload[name]).__name__}, expected {expected}"
                )
    return CheckResult(
        "schema",
        not failures,
        f"validated {len(payloads)} messages on {topic} against {len(check.fields)} fields",
        failures[:20],
    )


def check_equivalence(
    prod_bootstrap: str,
    prod_output_topic: str,
    canary_bootstrap: str,
    canary_output_topic: str,
    journal: Journal,
    check: EquivalenceCheck,
) -> CheckResult:
    """Compare canary vs prod output for every journaled mirrored input.

    Join key: the payload field named by `check.join_key` (same field the journal
    recorded). Later records win, so a reprocessed message is compared against its
    latest output on each side.
    """
    expected_join_values = set(journal.join_values())
    if not expected_join_values:
        return CheckResult("equivalence", False, "journal is empty; nothing to compare")

    # The canary should emit one record per mirrored input; wait for at least
    # that many before judging (the drain gives up after max_wait_s if fewer
    # arrive — missing keys are then reported as failures below).
    min_expected = journal.count()
    prod_by_key = {}
    for payload in _payloads(_drain_topic(prod_bootstrap, prod_output_topic)):
        if check.join_key in payload:
            prod_by_key[str(payload[check.join_key])] = payload

    canary_by_key = {}
    for payload in _payloads(
        _drain_topic(canary_bootstrap, canary_output_topic, min_expected=min_expected)
    ):
        if check.join_key in payload:
            canary_by_key[str(payload[check.join_key])] = payload

    failures = []
    compared = 0
    missing_prod = 0
    missing_canary = 0

    for join_value in sorted(expected_join_values):
        prod_out = prod_by_key.get(join_value)
        canary_out = canary_by_key.get(join_value)

        if canary_out is None:
            # Dropped or never processed by the canary: a hard failure.
            missing_canary += 1
            failures.append(f"join {join_value!r}: canary produced no output (message dropped?)")
            continue
        if prod_out is None:
            if check.require_prod_output:
                missing_prod += 1
                failures.append(
                    f"join {join_value!r}: no prod output found on {prod_output_topic}"
                )
                continue
            continue

        compared += 1
        field_failures = _compare_payloads(prod_out, canary_out, check)
        failures.extend(f"join {join_value!r}: {msg}" for msg in field_failures)

    summary = (
        f"compared {compared} records "
        f"(canary missing {missing_canary}, prod missing {missing_prod}) "
        f"prod={prod_output_topic} canary={canary_output_topic}"
    )
    return CheckResult(
        "equivalence",
        not failures,
        summary,
        failures[:20],
        compared=compared,
        missing_canary=missing_canary,
        missing_prod=missing_prod,
    )


def _compare_payloads(prod: dict, canary: dict, check: EquivalenceCheck) -> list[str]:
    failures = []
    ignored = set(check.ignore_fields)
    allowed = check.allow_diff
    keys = [k for k in prod if k not in ignored] + [k for k in canary if k not in ignored and k not in prod]
    for key in keys:
        pv = prod.get(key)
        cv = canary.get(key)
        if key in allowed:
            expectation = allowed[key]
            if expectation == "any":
                continue
            if expectation != cv:
                failures.append(f"field {key!r}: expected new value {expectation!r}, got {cv!r}")
            continue
        if isinstance(pv, bool) != isinstance(cv, bool):
            failures.append(f"field {key!r}: {pv!r} vs {cv!r} (type change)")
        elif isinstance(pv, (int, float)) and isinstance(cv, (int, float)):
            if abs(pv - cv) > check.numeric_tolerance:
                failures.append(f"field {key!r}: prod {pv} vs canary {cv}")
        elif pv != cv:
            failures.append(f"field {key!r}: prod {pv!r} vs canary {cv!r}")
    return failures


def group_lag(bootstrap: str, group: str, topic: str, partition: int) -> int:
    """Current committed lag of a consumer group on one partition.

    Returns the end offset when the group has no committed offset yet (full lag).
    """
    from confluent_kafka import ConsumerGroupTopicPartitions
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    tp = TopicPartition(topic, partition)
    request = ConsumerGroupTopicPartitions(group, topic_partitions=[tp])
    futures = admin.list_consumer_group_offsets([request], request_timeout=10.0)
    listing = futures[group].result(timeout=15.0)
    offsets = {(t.topic, t.partition): t.offset for t in listing.topic_partitions}
    offset = offsets.get((topic, partition), -1)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"stream-canary-lag-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        _, high = consumer.get_watermark_offsets(tp, timeout=10.0)
    finally:
        consumer.close()
    if offset < 0:
        return high
    return max(0, high - offset)


def check_lag(bootstrap: str, check: LagCheck) -> CheckResult:
    lag = group_lag(bootstrap, check.group, check.topic, check.partition)
    passed = lag <= check.max_lag
    return CheckResult(
        "lag",
        passed,
        f"group {check.group!r} lag on {check.topic}[{check.partition}] is {lag} (max {check.max_lag})",
    )
