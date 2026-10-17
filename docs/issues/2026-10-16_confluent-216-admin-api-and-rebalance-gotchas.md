# Confluent-kafka 2.16 admin API + rebalance-pause gotchas in verification

Date: 2026-10-16
Status: resolved

## Symptom

E2E rollout tests failed in two non-obvious ways against a real Kafka:

1. `TypeError: 'Future' object has no attribute 'valid'` / `'ConsumerGroupTopicPartitions'
   is not iterable` from lag checks.
2. Schema checks intermittently reported `no messages on <topic> to validate` on a topic
   that demonstrably had messages (the equivalence check right after read all of them).

## Root cause 1: confluent-kafka 2.16 changed the admin API shape

- `AdminClient.list_consumer_groups()` returns a **Future** — `.result(timeout).valid` holds
  the group listing.
- `AdminClient.list_consumer_group_offsets(requests)` takes a
  **`List[ConsumerGroupTopicPartitions]`** (built via `from confluent_kafka import
  ConsumerGroupTopicPartitions`), returns a **dict of Futures keyed by group id**, and each
  Future resolves to a **`ConsumerGroupTopicPartitions`** — a `.topic_partitions` list of
  `TopicPartition` with `.offset`, **not** a `{TopicPartition: offset}` dict.
- `AdminClient.list_topics()` takes no `request_timeout` kwarg (that's a Consumer/Producer
  option); use `list_topics(timeout=...)`.

## Root cause 2: an empty consume batch is not end-of-topic

A fresh consumer group's first `consume()` can return nothing for several seconds while the
group joins and rebalances (cp-kafka default `group.initial.rebalance.delay.ms=3000`).
Draining by "break on first empty batch" treated that pause as an empty topic.

Fix: `_drain_topic` breaks only after **two consecutive** empty windows.

## Root cause 3: verification racing a canary mid-rebalance

The orchestrator went straight from mirror to verify. If the canary was still rebalining
(notably when its consumer group name was reused from a previous rollout, so group
coordinator state lingers), output checks ran before it had produced anything.

Fix: an explicit **settle step** after mirroring — poll the canary consumer group's lag on
the canary input topic until it hits 0 (`verify.group_lag`), bounded by
`deploy.readiness_timeout_s`. Verification only runs against a canary that has demonstrably
drained its input.

## Resolution

- Lag and readiness checks go through `stream_canary/verify.py:group_lag` and
  `stream_canary/executor.py:CanaryHandle.wait_ready` using the 2.16 API shapes above.
- Drain logic uses the consecutive-empty-windows rule.
- The orchestrator settles before verify (`orchestrator.py`, between mirror and verify).

## Lesson

Consumer-side "read the topic" code in Kafka is never just a loop: group join/rebalance
timing introduces empty-but-not-finished states everywhere. Anything that must observe a
complete stream should either read to a known watermark or wait for a quiescence signal
(lag 0), never a single empty poll.
