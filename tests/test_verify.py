import pytest

from stream_canary.spec import EquivalenceCheck
from stream_canary.verify import _compare_payloads


def _check(**kwargs):
    defaults = {"join_key": "id", "ignore_fields": [], "allow_diff": {},
                "numeric_tolerance": 0}
    defaults.update(kwargs)
    return EquivalenceCheck(**defaults)


def test_identical_payloads_pass():
    prod = {"id": "a", "x": 1, "y": "same"}
    canary = {"id": "a", "x": 1, "y": "same"}
    assert _compare_payloads(prod, canary, _check()) == []


def test_field_diff_fails():
    prod = {"id": "a", "x": 1}
    canary = {"id": "a", "x": 2}
    failures = _compare_payloads(prod, canary, _check())
    assert len(failures) == 1 and "'x'" in failures[0]


def test_ignore_fields_skip_comparison():
    prod = {"id": "a", "processed_at_ms": 1, "x": 1}
    canary = {"id": "a", "processed_at_ms": 999, "x": 1}
    assert _compare_payloads(prod, canary, _check(ignore_fields=["processed_at_ms"])) == []


def test_allow_diff_any_accepts_any_new_value():
    prod = {"id": "a", "version": "1.0.0"}
    canary = {"id": "a", "version": "2.0.0"}
    assert _compare_payloads(prod, canary, _check(allow_diff={"version": "any"})) == []


def test_allow_diff_exact_value_enforced():
    prod = {"id": "a", "version": "1.0.0"}
    canary = {"id": "a", "version": "3.0.0"}
    failures = _compare_payloads(prod, canary, _check(allow_diff={"version": "2.0.1"}))
    assert len(failures) == 1 and "expected new value" in failures[0]


def test_numeric_tolerance():
    prod = {"id": "a", "latency_ms": 100.0}
    canary = {"id": "a", "latency_ms": 100.0000001}
    assert _compare_payloads(prod, canary, _check(numeric_tolerance=0.001)) == []
    failures = _compare_payloads(prod, canary, _check(numeric_tolerance=0.00000001))
    assert len(failures) == 1


def test_bool_is_not_int():
    prod = {"id": "a", "flag": 1}
    canary = {"id": "a", "flag": True}
    failures = _compare_payloads(prod, canary, _check())
    assert failures  # bool vs int must be flagged


def test_missing_field_on_canary_side():
    prod = {"id": "a", "x": 1, "y": 2}
    canary = {"id": "a", "x": 1}
    failures = _compare_payloads(prod, canary, _check())
    assert len(failures) == 1 and "'y'" in failures[0]


def test_new_field_on_canary_side_fails():
    prod = {"id": "a", "x": 1}
    canary = {"id": "a", "x": 1, "new_field": "surprise"}
    failures = _compare_payloads(prod, canary, _check())
    assert len(failures) == 1
