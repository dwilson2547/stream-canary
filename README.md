---
tier: experiment
domain: messaging
---

# stream-canary

Safe-rollout orchestrator for Kafka stream processors. Deploy a new service version as an
**isolated canary**, mirror a sample of prod traffic into it, **verify conditions** (schema
validity, output equivalence against prod, consumer health), pass through approval **gates**,
then either **promote** to prod or **roll back** with prod never having been touched by a bad
version.

Proof of concept for a deploy strategy developed against large-scale pub/sub systems at work:
partitions mirrored into an isolated env, sample-message verification, canary-vs-prod output
comparison, then rollout or teardown.

## Rollout model

```
deploy canary -> mirror prod partition -> verify -> gates -> promote | rollback -> teardown -> report
```

- **Mirror** taps a prod partition *without committing offsets* (prod consumer groups are never
  disturbed). Two modes: `replay` (latest N messages) and `live-tail` (dual-write new traffic
  for a duration). Every mirrored message is journaled with its prod offset and join key so
  outputs can be compared side by side.
- **Verify** runs declarative conditions against the canary:
  - `schema` — required fields / types on canary output
  - `equivalence` — for every journaled input, compare canary output against prod output for
    the same join key, with `ignore_fields`, `numeric_tolerance`, and `allow_diff` (fields that
    are *expected* to change in the new version)
  - `lag` — consumer-group lag ceiling on the canary
- **Gates** — steps where humans or other pipelines approve: `manual` (prompt) or `exec`
  (command whose exit code decides). A failing gate means rollback.
- **Promote / rollback** — `promote` runs the spec's promote command (restart prod on the new
  version); `rollback` tears the canary down. Prod is never modified before the verify + gate
  phase passes.
- **Report** — every rollout writes `report.json` + `report.md` and a journal under
  `.rollouts/<name>/<timestamp>/`.

## Rollout spec (YAML)

```yaml
name: upper-v2-rollout
broker: kafka
prod:
  bootstrap: localhost:9092
  input_topic: orders.raw
  output_topic: orders.enriched
  partition: 0
canary:
  bootstrap: localhost:9092
  input_topic: canary.orders.raw
  output_topic: canary.orders.enriched
deploy:
  runtime: local
  command: python examples/processors/upper/processor.py
  env:
    PROCESSOR_VERSION: "2.0.1"
    INPUT_TOPIC: canary.orders.raw
    OUTPUT_TOPIC: canary.orders.enriched
  consumer_group: canary-upper
mirror:
  mode: replay
  sample_size: 100
verify:
  - type: schema
    fields: {order_id: str, total_cents: int, status: str}
  - type: equivalence
    join_key: order_id
    ignore_fields: [processed_at_ms, version]
    allow_diff: {version: "2.0.1"}
  - type: lag
    group: canary-upper
    topic: canary.orders.raw
    max_lag: 10
gates:
  - type: manual
    prompt: "External validation pipeline green?"
on_success: promote
```

## Demo

`start.sh` brings up a single-node Kafka (compose), creates topics, runs the prod processor
v1, and seeds `orders.raw` with synthetic orders. Then:

```bash
# green path: v2.0.1 is a clean change -> verify passes -> promote
stream-canary rollout examples/rollouts/green.yaml

# red path: v2.0.0 contains an off-by-one regression -> equivalence fails -> auto-rollback
stream-canary rollout examples/rollouts/red.yaml
```

## Scope of the PoC

- Local runtime (`deploy.runtime: local`) launches the canary as a subprocess; a `k8s` runtime
  (canary in its own namespace on the home cluster, promote via helm upgrade) is the next
  milestone — the executor interface is already split for it.
- One broker driver (`kafka`); the broker layer is a thin seam, not a portability abstraction.

## Layout

- `stream_canary/spec.py` — rollout spec model and validation
- `stream_canary/mirror.py` — non-committing partition tap, replay + live-tail
- `stream_canary/journal.py` — sqlite journal of mirrored inputs (offset + join key)
- `stream_canary/verify.py` — schema / equivalence / lag conditions
- `stream_canary/gates.py` — manual + exec approval gates
- `stream_canary/orchestrator.py` — phase state machine
- `stream_canary/executor.py` — canary lifecycle (local runtime now, k8s next)
- `stream_canary/report.py` — rollout report (json + md)
- `examples/` — sample processor (v1 prod / v2 canary), seed script, green + red rollout specs
- `compose/kafka.yml` — demo Kafka
