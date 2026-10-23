"""CLI: validate and run a workflow file.

    rivet validate ci.json
    rivet run ci.json --context '{"branch": "main"}'
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .engine import Engine, Status
from .graph import CycleError, WorkflowError, topo_order
from .spec import from_dict


def load(path: str):
    raw = Path(path).read_text(encoding="utf-8")
    if path.endswith((".yaml", ".yml")):
        try:
            import yaml  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise SystemExit("PyYAML is required for .yaml workflows: pip install pyyaml") from exc
        data = yaml.safe_load(raw)
    else:
        data = json.loads(raw)
    return from_dict(data)


def cmd_validate(args) -> int:
    try:
        wf = load(args.path)
        order = topo_order(wf)
    except (WorkflowError, json.JSONDecodeError) as exc:
        print(f"invalid: {exc}", file=sys.stderr)
        return 1
    print(f"valid: {wf.name} — {len(order)} steps")
    for i, sid in enumerate(order, 1):
        print(f"  {i:>2}. {sid}")
    return 0


def cmd_run(args) -> int:
    try:
        wf = load(args.path)
    except (WorkflowError, json.JSONDecodeError) as exc:
        print(f"invalid: {exc}", file=sys.stderr)
        return 1

    ctx = json.loads(args.context) if args.context else {}
    icons = {
        Status.OK: "ok  ",
        Status.FAILED: "FAIL",
        Status.SKIPPED: "skip",
        Status.TIMED_OUT: "TIME",
    }

    def listener(sid, res):
        print(f"  [{icons[Status(res['status'])]}] {sid} ({res['elapsed']}s)")
        if res.get("error"):
            print(f"         {res['error']}")

    engine = Engine(listener=listener)
    try:
        run = engine.run(wf, ctx)
    except CycleError as exc:
        print(f"invalid: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(run.as_dict(), indent=2))
    print("run:", "ok" if run.ok else "FAILED")
    return 0 if run.ok else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="rivet", description="Run a DAG workflow.")
    sub = p.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validate", help="check the graph without running it")
    v.add_argument("path")
    v.set_defaults(func=cmd_validate)

    r = sub.add_parser("run", help="execute the workflow")
    r.add_argument("path")
    r.add_argument("--context", help="JSON context passed to every step")
    r.add_argument("--json", action="store_true", help="print the full run record")
    r.set_defaults(func=cmd_run)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
