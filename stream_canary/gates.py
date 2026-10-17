"""Approval gates: manual prompts and exec (external pipeline) checks."""

import logging
import subprocess
from dataclasses import dataclass

from .spec import ExecGate, ManualGate

log = logging.getLogger("stream_canary.gates")


@dataclass
class GateResult:
    gate_type: str
    passed: bool
    detail: str
    auto_approved: bool = False
    command: str | None = None
    exit_code: int | None = None


def run_manual_gate(gate: ManualGate, non_interactive: bool) -> GateResult:
    if non_interactive:
        log.info("manual gate auto-approved (--non-interactive): %s", gate.prompt)
        return GateResult("manual", True, gate.prompt, auto_approved=True)
    while True:
        answer = input(f"GATE: {gate.prompt} [y/n] ").strip().lower()
        if answer in ("y", "n"):
            passed = answer == "y"
            return GateResult("manual", passed, gate.prompt)


def run_exec_gate(gate: ExecGate, workdir: str | None = None) -> GateResult:
    log.info("exec gate: %s", gate.command)
    proc = subprocess.run(gate.command, shell=True, cwd=workdir, capture_output=True, text=True)
    detail = (proc.stdout + proc.stderr).strip()[-500:]
    passed = proc.returncode == 0
    if not passed:
        log.warning("exec gate FAILED (exit %d): %s", proc.returncode, gate.command)
    return GateResult(
        "exec", passed, detail or f"exit code {proc.returncode}",
        command=gate.command, exit_code=proc.returncode,
    )
