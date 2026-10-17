"""Rollout state machine: deploy -> mirror -> verify -> gates -> promote|rollback -> teardown."""

import logging
import subprocess
import time
from dataclasses import asdict
from pathlib import Path

from . import mirror as mirror_mod
from . import verify as verify_mod
from .executor import (
    CanaryHandle,
    delete_consumer_group,
    delete_topics,
    ensure_topics,
    start_canary,
)
from .gates import run_exec_gate, run_manual_gate
from .journal import Journal
from .report import RolloutReport, now_utc
from .spec import EquivalenceCheck, ExecGate, LagCheck, ManualGate, RolloutSpec, SchemaCheck

log = logging.getLogger("stream_canary.orchestrator")


def _equivalence_join_key(spec: RolloutSpec) -> str:
    for check in spec.verify:
        if isinstance(check, EquivalenceCheck):
            return check.join_key
    return "id"


def run_rollout(spec: RolloutSpec, non_interactive: bool = False) -> int:
    workdir = Path(spec.workdir_root) / spec.name / now_utc().replace(":", "").replace("+", "Z")
    workdir.mkdir(parents=True, exist_ok=True)
    report = RolloutReport(spec_name=spec.name, started_at=now_utc())
    canary: CanaryHandle | None = None
    journal: Journal | None = None
    exit_code = 1

    try:
        # ---- phase: deploy ------------------------------------------------
        report.failed_phase = "deploy"
        log.info("[%s] phase: deploy (runtime %s)", spec.name, spec.deploy.runtime)
        ensure_topics(spec.canary.bootstrap, [spec.canary.input_topic, spec.canary.output_topic])
        canary = start_canary(spec)
        canary.wait_ready()

        # ---- phase: mirror ------------------------------------------------
        report.failed_phase = "mirror"
        log.info("[%s] phase: mirror (%s)", spec.name, spec.mirror.mode)
        journal = Journal(str(workdir / "journal.db"), _equivalence_join_key(spec))
        if spec.mirror.mode == "replay":
            result = mirror_mod.replay(
                spec.prod.bootstrap, spec.prod.input_topic, spec.prod.partition,
                spec.mirror.sample_size,
                spec.canary.bootstrap, spec.canary.input_topic, journal,
            )
        else:
            result = mirror_mod.live_tail(
                spec.prod.bootstrap, spec.prod.input_topic, spec.prod.partition,
                spec.mirror.tail_duration_s, spec.mirror.idle_stop_s,
                spec.canary.bootstrap, spec.canary.input_topic, journal,
            )
        report.mirrored = result.mirrored
        report.prod_partition_range = [result.prod_low_offset, result.prod_high_offset]

        # ---- settle: wait for the canary to drain its input ----------------
        # The canary may still be mid-rebalance or processing; verifying before
        # it has consumed the mirrored input races the checks.
        from .verify import group_lag

        settle_deadline = time.monotonic() + spec.deploy.readiness_timeout_s
        while True:
            lag = group_lag(
                spec.canary.bootstrap, spec.deploy.consumer_group,
                spec.canary.input_topic, spec.prod.partition,
            )
            if lag == 0:
                break
            if time.monotonic() > settle_deadline:
                raise RuntimeError(
                    f"canary did not drain its input within "
                    f"{spec.deploy.readiness_timeout_s}s (lag {lag})"
                )
            time.sleep(1.0)
        log.info("[%s] canary input drained (lag 0)", spec.name)

        # ---- phase: verify ------------------------------------------------
        report.failed_phase = "verify"
        log.info("[%s] phase: verify (%d checks)", spec.name, len(spec.verify))
        all_passed = True
        for check in spec.verify:
            if isinstance(check, SchemaCheck):
                cr = verify_mod.check_schema(
                    spec.canary.bootstrap, spec.canary.output_topic, check,
                    min_expected=report.mirrored or 0,
                )
            elif isinstance(check, EquivalenceCheck):
                cr = verify_mod.check_equivalence(
                    spec.prod.bootstrap, spec.prod.output_topic,
                    spec.canary.bootstrap, spec.canary.output_topic,
                    journal, check,
                )
            elif isinstance(check, LagCheck):
                cr = verify_mod.check_lag(spec.canary.bootstrap, check)
            else:
                raise ValueError(f"unknown check type {check!r}")
            report.checks.append(asdict(cr))
            log.info("  check %s: %s — %s", cr.check_type, "PASS" if cr.passed else "FAIL", cr.summary)
            for failure in cr.failures:
                log.info("    %s", failure)
            all_passed = all_passed and cr.passed
        if not all_passed:
            raise RuntimeError("verification failed")

        # ---- phase: gates --------------------------------------------------
        report.failed_phase = "gates"
        log.info("[%s] phase: gates (%d)", spec.name, len(spec.gates))

        for gate in spec.gates:
            if isinstance(gate, ManualGate):
                gr = run_manual_gate(gate, non_interactive)
            elif isinstance(gate, ExecGate):
                gr = run_exec_gate(gate, str(workdir))
            else:
                raise ValueError(f"unknown gate type {gate!r}")
            report.gates.append(asdict(gr))
            log.info("  gate %s: %s", gr.gate_type, "PASS" if gr.passed else "FAIL")
            if not gr.passed:
                raise RuntimeError(f"gate refused the rollout: {gr.detail}")

        # ---- phase: promote -------------------------------------------------
        report.failed_phase = "promote"
        if spec.on_success == "verify-only":
            log.info("[%s] verify-only: leaving canary up for inspection", spec.name)
            report.outcome = "verify-only"
            report.failed_phase = None
            exit_code = 0
        else:
            if spec.promote_command:
                log.info("[%s] phase: promote — %s", spec.name, spec.promote_command)
                proc = subprocess.run(spec.promote_command, shell=True, capture_output=True, text=True)
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"promote command failed (exit {proc.returncode}): "
                        f"{(proc.stdout + proc.stderr).strip()[-500:]}"
                    )
            else:
                log.info("[%s] promote: no promote_command configured; canary verified", spec.name)
            report.outcome = "promoted"
            report.failed_phase = None
            exit_code = 0

    except Exception as exc:
        log.error("[%s] %s", spec.name, exc)
        report.outcome = "rolled_back"
        report.error = str(exc)
        exit_code = 1
    finally:
        # ---- phase: teardown ------------------------------------------------
        log.info("[%s] phase: teardown", spec.name)
        if canary is not None:
            canary.teardown()
        if spec.delete_topics_on_teardown and report.outcome != "verify-only":
            delete_topics(
                spec.canary.bootstrap,
                [spec.canary.input_topic, spec.canary.output_topic],
            )
            # Group state outlives the canary; a reused group name would make
            # the next rollout resume from stale committed offsets.
            try:
                delete_consumer_group(spec.canary.bootstrap, spec.deploy.consumer_group)
            except Exception as exc:
                log.warning("could not delete canary consumer group: %s", exc)
        if journal is not None:
            journal.close()
        report.finished_at = now_utc()
        report.write(str(workdir))
        log.info(
            "[%s] rollout %s — report at %s", spec.name, report.outcome.upper(),
            workdir / "report.md",
        )

    return exit_code
