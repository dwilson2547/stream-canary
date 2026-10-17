"""Non-committing prod partition tap: replay and live-tail mirror modes.

The mirror never commits offsets: it uses an ephemeral consumer with
enable.auto.commit=False and never calls commit(), so prod consumer groups,
rebalances, and lag are completely undisturbed.
"""

import logging
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from .journal import Journal

log = logging.getLogger("stream_canary.mirror")


class MirrorResult:
    def __init__(self):
        self.mirrored = 0
        self.prod_low_offset: int | None = None
        self.prod_high_offset: int | None = None
        self.duration_s: float = 0.0


def _ephemeral_consumer(bootstrap: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"stream-canary-mirror-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def replay(
    bootstrap: str,
    topic: str,
    partition: int,
    sample_size: int,
    canary_bootstrap: str,
    canary_topic: str,
    journal: Journal,
) -> MirrorResult:
    """Mirror the latest `sample_size` messages from one prod partition."""
    result = MirrorResult()
    started = time.monotonic()

    consumer = _ephemeral_consumer(bootstrap)
    try:
        tp = TopicPartition(topic, partition, 0)
        low, high = consumer.get_watermark_offsets(tp, timeout=10.0)
        if high == 0:
            raise RuntimeError(f"prod partition {topic}[{partition}] is empty; nothing to mirror")
        start = max(low, high - sample_size)
        result.prod_low_offset, result.prod_high_offset = start, high

        consumer.assign([TopicPartition(topic, partition, start)])
        producer = Producer({"bootstrap.servers": canary_bootstrap})

        n = 0
        while n < high - start:
            msg = consumer.poll(5.0)
            if msg is None:
                raise RuntimeError(
                    f"mirror timed out waiting for message {n} of {high - start} "
                    f"from {topic}[{partition}]@{start + n}"
                )
            if msg.error():
                raise KafkaException(msg.error())
            producer.produce(canary_topic, key=msg.key(), value=msg.value())
            journal.record(msg.offset(), msg.value())
            n += 1
        producer.flush(10.0)
        if producer.flush(1.0) > 0:
            raise RuntimeError("mirror producer could not flush all messages to canary")
        result.mirrored = n
    finally:
        consumer.close()

    result.duration_s = time.monotonic() - started
    log.info(
        "mirror replayed %d messages from %s[%d] offsets %d..%d in %.1fs",
        result.mirrored, topic, partition, result.prod_low_offset or 0,
        (result.prod_high_offset or 1) - 1, result.duration_s,
    )
    return result


def live_tail(
    bootstrap: str,
    topic: str,
    partition: int,
    tail_duration_s: float,
    idle_stop_s: float,
    canary_bootstrap: str,
    canary_topic: str,
    journal: Journal,
) -> MirrorResult:
    """Dual-write new traffic from one prod partition into the canary.

    Runs for at least `tail_duration_s`, stopping early once `idle_stop_s` of
    silence has elapsed after that.
    """
    result = MirrorResult()
    started = time.monotonic()

    consumer = _ephemeral_consumer(bootstrap)
    try:
        tp = TopicPartition(topic, partition, 0)
        _, high = consumer.get_watermark_offsets(tp, timeout=10.0)
        result.prod_low_offset = high
        consumer.assign([TopicPartition(topic, partition, high)])
        producer = Producer({"bootstrap.servers": canary_bootstrap})

        last_message = time.monotonic()
        while True:
            now = time.monotonic()
            elapsed = now - started
            idle = now - last_message
            if elapsed >= tail_duration_s and idle >= idle_stop_s:
                break
            if elapsed >= tail_duration_s + idle_stop_s:
                break
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            producer.produce(canary_topic, key=msg.key(), value=msg.value())
            journal.record(msg.offset(), msg.value())
            result.mirrored += 1
            last_message = time.monotonic()
        producer.flush(10.0)
        if producer.flush(1.0) > 0:
            raise RuntimeError("mirror producer could not flush all messages to canary")
        _, end_high = consumer.get_watermark_offsets(tp, timeout=10.0)
        result.prod_high_offset = end_high
    finally:
        consumer.close()

    result.duration_s = time.monotonic() - started
    log.info(
        "mirror live-tailed %d messages from %s[%d] in %.1fs",
        result.mirrored, topic, partition, result.duration_s,
    )
    return result
