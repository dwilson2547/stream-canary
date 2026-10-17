#!/usr/bin/env bash
# Start the stream-canary demo environment: Kafka, prod processor v1, seeded orders.
set -euo pipefail
cd "$(dirname "$0")"

if [ -f .prod-processor.pid ] && kill -0 "$(cat .prod-processor.pid)" 2>/dev/null; then
  echo "prod processor already running (pid $(cat .prod-processor.pid))"
  exit 0
fi

echo "== starting kafka =="
docker compose -f compose/kafka.yml up -d
for i in $(seq 1 60); do
  if docker compose -f compose/kafka.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "== starting prod processor (v1.4.2) =="
BOOTSTRAP=localhost:9092 \
INPUT_TOPIC=orders.raw OUTPUT_TOPIC=orders.enriched \
GROUP=upper-prod PROCESSOR_VERSION=1.4.2 \
  nohup uv run python examples/processors/upper/processor.py --role prod \
  > .prod-processor.log 2>&1 &
echo $! > .prod-processor.pid

echo "== seeding orders =="
uv run python examples/seed/seed.py 200

echo
echo "demo env up: kafka on localhost:9092, prod processor v1.4.2 on orders.raw -> orders.enriched"
echo "try: stream-canary rollout examples/rollouts/green.yaml   (or red.yaml)"
