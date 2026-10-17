"""Seed the demo prod topics: create topics and produce synthetic orders."""

import json
import os
import random
import sys

from confluent_kafka import Producer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from stream_canary.executor import ensure_topics  # noqa: E402

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092")
PROD_INPUT = "orders.raw"
PROD_OUTPUT = "orders.enriched"


def make_order(i: int, rng: random.Random) -> dict:
    n_items = rng.randint(1, 8)
    items = [
        {
            "sku": f"SKU-{rng.randint(1000, 9999)}",
            "qty": rng.randint(1, 4),
            "unit_cents": rng.randint(50, 9000),
        }
        for _ in range(n_items)
    ]
    return {"order_id": f"ord-{i:04d}", "customer": f"cust-{i:03d}", "items": items}


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    rng = random.Random(42)
    ensure_topics(BOOTSTRAP, [PROD_INPUT, PROD_OUTPUT])

    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for i in range(count):
        order = make_order(i, rng)
        producer.produce(
            PROD_INPUT, key=order["order_id"].encode(), value=json.dumps(order).encode()
        )
    producer.flush(15.0)
    print(f"seeded {count} orders into {PROD_INPUT}")


if __name__ == "__main__":
    main()
