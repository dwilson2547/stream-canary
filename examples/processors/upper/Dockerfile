# Sample processor image for the stream-canary PoC.
# The service version is injected at runtime via PROCESSOR_VERSION, so one build
# plays prod v1 and canary v2; the tag records which version a deployment runs.
FROM python:3.12-slim

RUN pip install --no-cache-dir confluent-kafka==2.16.0

COPY examples/processors/upper/processor.py /app/processor.py

ENTRYPOINT ["python", "/app/processor.py"]
