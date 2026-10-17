#!/usr/bin/env bash
# Bring up the stream-canary PoC stack (namespace, Kafka, prod processor),
# create the prod topics, and seed orders from the workstation.
# Hand-applied sandbox: nothing here is ArgoCD-managed.
set -euo pipefail
cd "$(dirname "$0")/../.."   # project root

BOOTSTRAP=${BOOTSTRAP:-192.168.0.35:31992}

echo "== applying PoC stack =="
kubectl apply -f k8s/poc/

echo "== waiting for kafka =="
for i in $(seq 1 90); do
  if uv run python -c "
from confluent_kafka.admin import AdminClient
AdminClient({'bootstrap.servers': '$BOOTSTRAP'}).list_topics(timeout=2.0)
" 2>/dev/null; then break; fi
  sleep 1
done

echo "== seeding orders =="
BOOTSTRAP=$BOOTSTRAP uv run python examples/seed/seed.py 200

echo
echo "PoC stack up: kafka at $BOOTSTRAP, prod processor v1.4.2 in namespace stream-canary"
echo "try: uv run stream-canary rollout examples/rollouts/green-k8s.yaml"
echo "teardown: kubectl delete namespace stream-canary"
