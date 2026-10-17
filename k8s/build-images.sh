#!/usr/bin/env bash
# Build and push the sample processor image, one tag per service version.
# The image itself is identical across versions (version is runtime env); tags
# exist so the k8s promote step (kubectl set image) has something to point at.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=dwilson2547/stream-canary-processor
docker build -t "$IMAGE" -f examples/processors/upper/Dockerfile .
for tag in 1.4.2 2.0.0 2.0.1; do
  docker tag "$IMAGE" "$IMAGE:$tag"
  docker push "$IMAGE:$tag"
done
echo "pushed $IMAGE:{1.4.2,2.0.0,2.0.1}"
