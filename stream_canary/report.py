"""Rollout report: JSON + markdown artifacts per rollout run."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class RolloutReport:
    spec_name: str
    started_at: str
    finished_at: str | None = None
    # promoted | rolled_back | verify-only | failed
    outcome: str = "running"
    failed_phase: str | None = None
    error: str | None = None
    mirrored: int | None = None
    prod_partition_range: list[int] | None = None
    checks: list[dict] = field(default_factory=list)
    gates: list[dict] = field(default_factory=list)

    def write(self, workdir: str) -> None:
        path = Path(workdir)
        path.mkdir(parents=True, exist_ok=True)
        (path / "report.json").write_text(json.dumps(asdict(self), indent=2) + "\n")

        lines = [
            f"# Rollout report: {self.spec_name}",
            "",
            f"- started: {self.started_at}",
        ]
        if self.finished_at:
            lines.append(f"- finished: {self.finished_at}")
        lines += [
            f"- outcome: **{self.outcome}**",
        ]
        if self.failed_phase:
            lines.append(f"- failed phase: {self.failed_phase}")
        if self.error:
            lines.append(f"- error: `{self.error}`")
        if self.mirrored is not None:
            lines.append(f"- messages mirrored: {self.mirrored}")
        if self.prod_partition_range:
            lines.append(
                f"- prod partition offsets mirrored: {self.prod_partition_range[0]}..{self.prod_partition_range[1] - 1}"
            )
        if self.checks:
            lines += ["", "## Checks", "", "| check | passed | detail |", "|---|---|---|"]
            for c in self.checks:
                lines.append(f"| {c['check_type']} | {'pass' if c['passed'] else 'FAIL'} | {c['summary']} |")
                for f in c.get("failures", []):
                    lines.append(f"| | | {f} |")
        if self.gates:
            lines += ["", "## Gates", "", "| gate | passed | detail |", "|---|---|---|"]
            for g in self.gates:
                lines.append(f"| {g['gate_type']} | {'pass' if g['passed'] else 'FAIL'} | {g['detail']} |")
        (path / "report.md").write_text("\n".join(lines) + "\n")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
