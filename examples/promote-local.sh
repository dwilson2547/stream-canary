#!/usr/bin/env bash
# Local promote: restart the prod processor on the new version.
# Called by the green rollout spec after verify + gates pass. The canary runs
# with --role canary, so only the prod process is targeted.
set -euo pipefail
cd "$(dirname "$0")/.."   # project root

NEW_VERSION="${NEW_VERSION:-2.0.1}"
pkill -f "processor.py --role prod" 2>/dev/null || true
sleep 2

BOOTSTRAP=localhost:9092 \
INPUT_TOPIC=orders.raw OUTPUT_TOPIC=orders.enriched \
GROUP=upper-prod PROCESSOR_VERSION="$NEW_VERSION" \
  nohup uv run python examples/processors/upper/processor.py --role prod \
  >> .prod-processor.log 2>&1 &
echo $! > .prod-processor.pid
echo "prod processor promoted to v$NEW_VERSION (pid $(cat .prod-processor.pid))"
