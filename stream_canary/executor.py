"""Canary lifecycle management: local (subprocess) and k8s (ephemeral Deployment) runtimes.

The k8s runtime creates a per-rollout Deployment via `kubectl apply` and removes it
via `kubectl delete`. These resources are deliberately NOT ArgoCD-managed: they are
short-lived, orchestrator-driven, and live in the PoC namespace, so nothing ArgoCD
owns is ever touched.
"""

import json
import logging
import os
import shlex
import subprocess
import time

import yaml
from confluent_kafka.admin import AdminClient, NewTopic

from .spec import CanaryDeploy, RolloutSpec

log = logging.getLogger("stream_canary.executor")


class CanaryHandle:
    """A started canary. `wait_ready()` blocks until it is consuming;
    `teardown()` must be idempotent."""

    def __init__(self, deploy: CanaryDeploy, canary_bootstrap: str):
        self.deploy = deploy
        self.canary_bootstrap = canary_bootstrap

    def wait_ready(self) -> None:
        """Wait until the canary consumer group exists (i.e. the service is consuming)."""
        deadline = time.monotonic() + self.deploy.readiness_timeout_s
        admin = AdminClient({"bootstrap.servers": self.canary_bootstrap})
        while time.monotonic() < deadline:
            listing = admin.list_consumer_groups(request_timeout=5.0).result(timeout=10.0)
            groups = listing.valid
            if any(g.group_id == self.deploy.consumer_group for g in groups):
                log.info("canary consumer group %r is up", self.deploy.consumer_group)
                return
            self._check_alive()
            time.sleep(1.0)
        raise RuntimeError(
            f"canary not ready after {self.deploy.readiness_timeout_s}s "
            f"(no consumer group {self.deploy.consumer_group!r})"
        )

    def _check_alive(self) -> None:
        pass

    def teardown(self) -> None:
        raise NotImplementedError


class LocalCanary(CanaryHandle):
    def __init__(self, deploy: CanaryDeploy, canary_bootstrap: str,
                 popen: subprocess.Popen):
        super().__init__(deploy, canary_bootstrap)
        self.popen = popen

    def _check_alive(self) -> None:
        if self.popen.poll() is not None:
            raise RuntimeError(
                f"canary process exited early with code {self.popen.returncode}"
            )

    def teardown(self) -> None:
        if self.popen.poll() is None:
            self.popen.terminate()
            try:
                self.popen.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.popen.kill()
                self.popen.wait(timeout=15)
            log.info("canary process terminated")
        else:
            log.info("canary process already stopped")


class K8sCanary(CanaryHandle):
    def __init__(self, deploy: CanaryDeploy, canary_bootstrap: str, rollout_name: str):
        super().__init__(deploy, canary_bootstrap)
        self.rollout_name = rollout_name

    def _kubectl(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        cmd = ["kubectl"]
        if self.deploy.k8s.kubecontext:
            cmd += ["--context", self.deploy.k8s.kubecontext]
        cmd += list(args)
        return subprocess.run(cmd, capture_output=True, text=True, check=check)

    def _check_alive(self) -> None:
        proc = self._kubectl(
            "get", "deployment", self.deploy.k8s.name,
            "-n", self.deploy.k8s.namespace,
            "-o", "jsonpath={.status.readyReplicas}",
            check=False,
        )
        if proc.returncode != 0 or proc.stdout.strip() in ("", "0"):
            raise RuntimeError(
                f"canary deployment {self.deploy.k8s.namespace}/{self.deploy.k8s.name} "
                f"has no ready replicas (kubectl said: {proc.stdout.strip()!r})"
            )

    def teardown(self) -> None:
        proc = self._kubectl(
            "delete", "deployment", self.deploy.k8s.name,
            "-n", self.deploy.k8s.namespace,
            "--ignore-not-found=true", "--wait=true",
            check=False,
        )
        if proc.returncode == 0:
            log.info("canary deployment %s/%s deleted",
                     self.deploy.k8s.namespace, self.deploy.k8s.name)
        else:
            log.warning("failed to delete canary deployment: %s", proc.stderr.strip())


def canary_deployment_manifest(spec: RolloutSpec) -> dict:
    """The canary Deployment, generated from the rollout spec.

    `deploy.k8s.image` is the canary version's image; `deploy.env` carries its
    configuration (topics, group, version) exactly like the local runtime.
    """
    deploy = spec.deploy
    if deploy.k8s is None:
        raise ValueError("deploy.k8s is required to build a canary deployment")
    name = deploy.k8s.name
    labels = {
        "app": name,
        "app.kubernetes.io/managed-by": "stream-canary",
        "stream-canary-rollout": spec.name,
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": deploy.k8s.namespace,
            "labels": labels,
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "containers": [
                        {
                            "name": "service",
                            "image": deploy.k8s.image,
                            "imagePullPolicy": "Always",
                            "env": [
                                {"name": k, "value": v} for k, v in deploy.env.items()
                            ],
                        }
                    ],
                },
            },
        },
    }


def start_k8s_canary(spec: RolloutSpec) -> CanaryHandle:
    deploy = spec.deploy
    if deploy.k8s is None:
        raise ValueError("deploy.k8s is required for runtime 'k8s'")
    manifest = yaml.safe_dump(canary_deployment_manifest(spec))
    log.info(
        "starting canary deployment %s/%s (image %s)",
        deploy.k8s.namespace, deploy.k8s.name, deploy.k8s.image,
    )
    cmd = ["kubectl"]
    if deploy.k8s.kubecontext:
        cmd += ["--context", deploy.k8s.kubecontext]
    cmd += ["apply", "-f", "-"]
    proc = subprocess.run(cmd, input=manifest, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"kubectl apply failed: {proc.stderr.strip()}")
    log.info("kubectl apply: %s", proc.stdout.strip())

    wait_cmd = [
        "kubectl",
        *(["--context", deploy.k8s.kubecontext] if deploy.k8s.kubecontext else []),
        "rollout", "status", f"deployment/{deploy.k8s.name}",
        "-n", deploy.k8s.namespace,
        f"--timeout={int(deploy.readiness_timeout_s)}s",
    ]
    wait = subprocess.run(wait_cmd, capture_output=True, text=True)
    if wait.returncode != 0:
        raise RuntimeError(f"canary deployment did not become ready: {wait.stderr.strip()}")
    log.info("canary deployment ready: %s", wait.stdout.strip())

    return K8sCanary(deploy, spec.canary.bootstrap, spec.name)


def start_local_canary(spec: RolloutSpec) -> CanaryHandle:
    deploy = spec.deploy
    env = dict(os.environ)
    env.update(deploy.env)
    log.info("starting canary: %s", deploy.command)
    popen = subprocess.Popen(
        shlex.split(deploy.command), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    return LocalCanary(deploy, spec.canary.bootstrap, popen)


def start_canary(spec: RolloutSpec) -> CanaryHandle:
    if spec.deploy.runtime == "local":
        return start_local_canary(spec)
    return start_k8s_canary(spec)


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


def delete_consumer_group(bootstrap: str, group: str, verify_timeout_s: float = 60.0) -> None:
    """Delete a consumer group and wait until it disappears from the cluster.

    Group state (committed offsets, coordinator membership) outlives the pods
    that created it. Reusing a group name in the next rollout without deleting
    it makes the new consumer resume from stale committed offsets instead of
    starting at earliest — a classic silent data-skip.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    listing = admin.list_consumer_groups(request_timeout=10.0).result(timeout=15.0)
    known = {g.group_id for g in listing.valid}
    if group not in known:
        return
    futures = admin.delete_consumer_groups([group], request_timeout=30.0)
    futures[group].result(timeout=30.0)
    log.info("deleted consumer group %s", group)

    deadline = time.monotonic() + verify_timeout_s
    while time.monotonic() < deadline:
        listing = admin.list_consumer_groups(request_timeout=10.0).result(timeout=15.0)
        if group not in {g.group_id for g in listing.valid}:
            log.info("consumer group removal confirmed: %s", group)
            return
        time.sleep(1.0)
    raise RuntimeError(f"consumer group {group!r} still present after {verify_timeout_s}s")

def delete_topics(bootstrap: str, topics: list[str], verify_timeout_s: float = 60.0) -> None:
    """Delete topics AND wait until they are gone from broker metadata.

    Kafka topic deletion is asynchronous: a topic can still appear in metadata
    (and even accept writes) for a while after a successful delete. Returning
    before removal is confirmed lets a subsequent rollout's ensure_topics skip
    creation and write into a dying topic.
    """
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
        deadline = time.monotonic() + verify_timeout_s
        while time.monotonic() < deadline:
            remaining = doomed and (set(admin.list_topics(timeout=5.0).topics.keys()) & set(doomed))
            if not remaining:
                log.info("topic removal confirmed: %s", ", ".join(doomed))
                return
            time.sleep(1.0)
        raise RuntimeError(
            f"topics still in broker metadata after {verify_timeout_s}s: {sorted(remaining)}"
        )
