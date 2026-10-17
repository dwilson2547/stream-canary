"""Sample stream processor for the stream-canary demo.

Consumes raw order JSON, computes the order total, and produces an enriched
record. Version history:

- v1.x  — original prod behavior
- v2.0.0 — REGRESSION: orders with more than 5 line items get a total off by
  one cent (schema-valid, only detectable by output comparison)
- v2.0.1 — fix

The version comes from PROCESSOR_VERSION so the same script plays prod v1 and
canary v2. `--role` is only a command-line marker so shell scripts can target
prod vs canary processes by name.
"""

import argparse
import json
import logging
import os
import time
import uuid

from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s processor: %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("processor")


def process(payload: dict, version: str) -> dict:
    total = sum(item["qty"] * item["unit_cents"] for item in payload["items"])
    if version == "2.0.0" and len(payload["items"]) > 5:
        # The regression: silently one cent short on large orders.
        total -= 1
    enriched = dict(payload)
    enriched["total_cents"] = total
    enriched["status"] = "ok" if total > 0 else "rejected"
    enriched["version"] = version
    enriched["processed_at_ms"] = int(time.time() * 1000)
    return enriched


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", default="worker", help="marker only: prod | canary")
    args = parser.parse_args()

    bootstrap = os.environ.get("BOOTSTRAP", "localhost:9092")
    input_topic = os.environ["INPUT_TOPIC"]
    output_topic = os.environ["OUTPUT_TOPIC"]
    group = os.environ["GROUP"]
    version = os.environ["PROCESSOR_VERSION"]

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": bootstrap})
    consumer.subscribe([input_topic])
    log.info(
        "%s v%s consuming %s -> %s (group %s)",
        args.role, version, input_topic, output_topic, group,
    )

    while True:
        batch = consumer.consume(num_messages=50, timeout=1.0)
        for msg in batch:
            if msg.error():
                continue
            payload = json.loads(msg.value())
            enriched = process(payload, version)
            producer.produce(
                output_topic, key=payload["order_id"].encode(),
                value=json.dumps(enriched).encode(),
            )
        producer.poll(0)


if __name__ == "__main__":
    main()
