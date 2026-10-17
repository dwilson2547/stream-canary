# k8s canary PoC: async topic deletion and consumer-group reuse poisoned back-to-back rollouts

Date: 2026-10-16
Status: resolved

## Symptom

On the home cluster (dedicated `stream-canary` namespace, single-pod KRaft Kafka), running the
red rollout immediately after the green rollout failed verification with the *wrong* signature:

- Run 1: red canary's equivalence failures showed `version: expected '2.0.0', got '2.0.1'` —
  outputs from the *previous* (green) canary were in the red run's canary topic. Schema had
  validated 223 messages where 200 were expected.
- Run 2 (after the first fix): red canary produced only 90 of 200 outputs
  (`canary missing 110`) — it silently resumed from a stale committed offset and skipped
  most of the mirrored input.

Both are cross-run contamination on the same broker: green's teardown left state that bled
into red's deploy/mirror/verify.

## Root cause 1: Kafka topic deletion is asynchronous

`AdminClient.delete_topics` futures resolve when the delete is *accepted*, not when the topic
is gone from metadata. The green run's teardown deleted `canary.orders.*`, red's
`ensure_topics` a second later still saw them in metadata, skipped creation, and the mirror
wrote into the dying topic — green's outputs were still readable in it.

**Fix:** `executor.delete_topics` now polls broker metadata until the topics are actually
absent (60s bound), and raises if they never disappear.

## Root cause 2: consumer-group state outlives the pods

Reusing the canary consumer group name across rollouts means the next canary joins a group
that may carry committed offsets (and coordinator membership) from the previous run. When the
recreated topic briefly still had the old offsets, the new canary resumed mid-topic instead
of consuming from earliest — it processed only 90 of 200 mirrored messages, silently.

**Fix:** teardown deletes the canary consumer group after the topics
(`executor.delete_consumer_group`, verified the same way). Belt and braces: the verify drains
now take `min_expected` (the mirrored count) so a canary that is still producing — or that
dropped messages — is measured against what should be there, not against silence
(`verify._drain_topic`: wait for `min_expected` records, then two empty windows, with a
`max_wait_s` give-up that lets equivalence report the missing keys as failures).

## Why the local demo never showed this

The compose-backed runs delete topics too, but the testcontainers e2e recreated state more
slowly and the local demo timing never hit the metadata-lag window. Only the real cluster,
with back-to-back CLI rollouts on a warm broker, exposed both races.

## Lesson

For any orchestrator that creates and destroys Kafka resources per run: teardown must verify
deletion (topics AND consumer groups) before the next run may reuse names. "Delete accepted"
is not "gone", and a named consumer group is never empty state — it is committed offsets
waiting to skip your data.

## Resolution

- `stream_canary/executor.py`: `delete_topics` and `delete_consumer_group` verify removal.
- `stream_canary/verify.py`: `_drain_topic(min_expected, max_wait_s)`; schema check takes
  the mirrored count, equivalence check takes the journal count.
- Verified live: green→red back-to-back on the cluster now produces the correct red-path
  signature (equivalence catches the planted `total_cents` regression, canary missing 0).
