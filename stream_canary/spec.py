"""Rollout spec model and validation."""

from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, model_validator


class ProdSide(BaseModel):
    bootstrap: str
    input_topic: str
    output_topic: str
    partition: int = 0
    # Prod service's consumer group: only read for lag checks, never committed to.
    consumer_group: str | None = None


class CanarySide(BaseModel):
    bootstrap: str
    input_topic: str
    output_topic: str


class K8sDeploy(BaseModel):
    namespace: str
    chart: str | None = None
    values_file: str | None = None
    image: str | None = None


class CanaryDeploy(BaseModel):
    runtime: Literal["local", "k8s"] = "local"
    # local: shell command that starts the canary service.
    command: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    consumer_group: str = "canary"
    # How long to wait for the canary consumer group to appear before failing the deploy.
    readiness_timeout_s: float = 60.0
    k8s: K8sDeploy | None = None

    @model_validator(mode="after")
    def runtime_requirements(self) -> "CanaryDeploy":
        if self.runtime == "local" and not self.command:
            raise ValueError("deploy.command is required for runtime 'local'")
        if self.runtime == "k8s" and not self.k8s:
            raise ValueError("deploy.k8s is required for runtime 'k8s'")
        return self


class MirrorSpec(BaseModel):
    mode: Literal["replay", "live-tail"] = "replay"
    # replay: latest N messages on the prod partition.
    sample_size: int = 100
    # live-tail: dual-write new traffic until the duration elapses or the stream goes quiet.
    tail_duration_s: float = 30.0
    idle_stop_s: float = 10.0


class SchemaCheck(BaseModel):
    type: Literal["schema"] = "schema"
    # Field name -> "str" | "int" | "float" | "bool". Validates presence and type on
    # every message read from the canary output topic.
    fields: dict[str, str]

    @model_validator(mode="after")
    def field_types(self) -> "SchemaCheck":
        allowed = {"str", "int", "float", "bool"}
        for name, t in self.fields.items():
            if t not in allowed:
                raise ValueError(f"schema field {name}: unknown type {t!r} (allowed: {sorted(allowed)})")
        return self


class EquivalenceCheck(BaseModel):
    type: Literal["equivalence"] = "equivalence"
    # Field in the message payload that identifies the logical record; used to join
    # canary output to prod output for the same mirrored input.
    join_key: str
    # Fields excluded from comparison entirely (timestamps, trace ids).
    ignore_fields: list[str] = Field(default_factory=list)
    # Fields the new version is EXPECTED to change: "any", or the exact expected value.
    allow_diff: dict[str, Union[str, int, float, bool]] = Field(default_factory=dict)
    # Max absolute difference for numeric fields outside ignore/allow sets.
    numeric_tolerance: float = 0
    # Whether a mirrored input with no matching prod output is a failure. Prod outputs
    # for replayed messages must exist from their original live processing.
    require_prod_output: bool = True


class LagCheck(BaseModel):
    type: Literal["lag"] = "lag"
    group: str
    topic: str
    partition: int = 0
    max_lag: int = 0


Check = Annotated[
    Union[SchemaCheck, EquivalenceCheck, LagCheck],
    Field(discriminator="type"),
]


class ManualGate(BaseModel):
    type: Literal["manual"] = "manual"
    prompt: str = "Approve rollout?"


class ExecGate(BaseModel):
    type: Literal["exec"] = "exec"
    # Command whose exit code decides the gate: 0 = approved, non-zero = veto (rollback).
    command: str


Gate = Annotated[Union[ManualGate, ExecGate], Field(discriminator="type")]


class RolloutSpec(BaseModel):
    name: str
    broker: Literal["kafka"] = "kafka"
    prod: ProdSide
    canary: CanarySide
    deploy: CanaryDeploy
    mirror: MirrorSpec = MirrorSpec()
    verify: list[Check] = Field(default_factory=list)
    gates: list[Gate] = Field(default_factory=list)
    # promote: run promote_command after gates pass.
    # verify-only: stop after gates, leave the canary up for inspection.
    on_success: Literal["promote", "verify-only"] = "promote"
    promote_command: str | None = None
    on_failure: Literal["rollback"] = "rollback"
    # Delete the canary topics as part of teardown (local broker reuse depends on this).
    delete_topics_on_teardown: bool = True
    workdir_root: str = ".rollouts"


def load_spec(path: str) -> RolloutSpec:
    import yaml

    with open(path) as f:
        raw = yaml.safe_load(f)
    return RolloutSpec.model_validate(raw)
