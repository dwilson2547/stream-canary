"""stream-canary CLI."""

import argparse
import logging
import sys

from .orchestrator import run_rollout
from .spec import load_spec


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="stream-canary",
        description="Safe-rollout orchestrator for Kafka stream processors",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p_rollout = sub.add_parser("rollout", help="run a rollout from a spec file")
    p_rollout.add_argument("spec", help="path to the rollout spec YAML")
    p_rollout.add_argument(
        "--non-interactive", action="store_true",
        help="auto-approve manual gates (records auto-approval in the report)",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.command == "rollout":
        spec = load_spec(args.spec)
        return run_rollout(spec, non_interactive=args.non_interactive)
    return 2


if __name__ == "__main__":
    sys.exit(main())
