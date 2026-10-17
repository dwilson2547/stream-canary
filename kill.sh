#!/usr/bin/env bash
# Stop the stream-canary demo environment.
set -uo pipefail
cd "$(dirname "$0")"

if [ -f .prod-processor.pid ]; then
  kill "$(cat .prod-processor.pid)" 2>/dev/null || true
  rm -f .prod-processor.pid
fi
# Belt and braces: any stray processor left over from a promote step.
pkill -f "processor.py --role prod" 2>/dev/null || true

docker compose -f compose/kafka.yml down 2>/dev/null || true
echo "demo env down"
