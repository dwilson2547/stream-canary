"""Canary lifecycle management. Local runtime now; k8s runtime is the next milestone."""

import logging
import os
import shlex
import subprocess
import time

from confluent_kafka.admin import AdminClient, NewTopic

from .spec import CanaryDeploy, RolloutSpec

log = logging.getLogger("stream_canary.executor")


class CanaryHandle:
    """A started canary deployment. `teardown()` must be idempotent."""

    def __init__(self, deploy: CanaryDeploy, popen: subprocess.Popen | None):
        self.deploy = deploy
        self.popen = popen

    def wait_ready(self) -> None:
        """Wait until the canary consumer group exists (i.e. the service is consuming)."""
        deadline = time.monotonic() + self.deploy.readiness_timeout_s
        admin = AdminClient({"bootstrap.servers": self.deploy_env_admin_bootstrap()})
        while time.monotonic() < deadline:
            listing = admin.list_consumer_groups(request_timeout=5.0).result(timeout=10.0)
            groups = listing.valid
            if any(g.group_id == self.deploy.consumer_group for g in groups):
                log.info("canary consumer group %r is up", self.deploy.consumer_group)
                return
            if self.popen is not None and self.popen.poll() is not None:
                raise RuntimeError(
                    f"canary process exited early with code {self.popen.returncode}"
                )
            time.sleep(1.0)
        raise RuntimeError(
            f"canary not ready after {self.deploy.readiness_timeout_s}s "
            f"(no consumer group {self.deploy.consumer_group!r})"
        )

    def deploy_env_admin_bootstrap(self) -> str:
        # The canary's broker is where its consumer group registers.
        return os.environ.get("_CANARY_BOOTSTRAP", "localhost:9092")

    def teardown(self) -> None:
        if self.popen is not None and self.popen.poll() is None:
            self.popen.terminate()
            try:
                self.popen.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.popen.kill()
                self.popen.wait(timeout=15)
            log.info("canary process terminated")
        else:
            log.info("canary process already stopped")


def ensure_topics(bootstrap: str, topics: list[str], partitions: int = 1) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics().topics.keys())
    missing = [NewTopic(t, num_partitions=partitions) for t in topics if t not in existing]
    if missing:
        for topic, fut in admin.create_topics(missing, request_timeout=30.0).items():
            try:
                fut.result()
                log.info("created topic %s", topic)
            except Exception as exc:  # confluent raises KafkaException subclasses
                if "TOPIC_ALREADY_EXISTS" in str(exc):
                    continue
                raise


def delete_topics(bootstrap: str, topics: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics().topics.keys())
    doomed = [t for t in topics if t in existing]
    if doomed:
        for topic, fut in admin.delete_topics(doomed, request_timeout=30.0).items():
            try:
                fut.result()
                log.info("deleted topic %s", topic)
            except Exception as exc:
                log.warning("failed to delete topic %s: %s", topic, exc)


def start_local_canary(spec: RolloutSpec) -> CanaryHandle:
    deploy = spec.deploy
    env = dict(os.environ)
    env.update(deploy.env)
    log.info("starting canary: %s", deploy.command)
    popen = subprocess.Popen(
        shlex.split(deploy.command), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    os.environ["_CANARY_BOOTSTRAP"] = spec.canary.bootstrap
    return CanaryHandle(deploy, popen)
