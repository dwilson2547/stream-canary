import json

import pytest

from stream_canary.journal import Journal


@pytest.fixture
def journal(tmp_path):
    j = Journal(str(tmp_path / "journal.db"), join_key="order_id")
    yield j
    j.close()


def _order(i: int) -> bytes:
    return json.dumps({"order_id": f"ord-{i:04d}", "items": []}).encode()


def test_record_and_join_values(journal):
    # join_values comes back in prod-offset order, not insertion order.
    journal.record(11, _order(1))
    journal.record(10, _order(2))
    assert journal.join_values() == ["ord-0002", "ord-0001"]
    assert journal.count() == 2


def test_join_key_must_be_present(journal):
    with pytest.raises(KeyError):
        journal.record(10, json.dumps({"other": 1}).encode())


def test_null_payload_rejected(journal):
    with pytest.raises(ValueError):
        journal.record(10, None)


def test_rejoin_same_key_replaces(journal):
    journal.record(10, _order(1))
    journal.record(11, _order(1))
    assert journal.count() == 1
