"""Workflow definition: load from YAML-like dicts, plus a JSON schema surface.

Kept as plain dicts so a workflow can come from a file, an API call or a test
without three different code paths.
"""
from __future__ import annotations

from typing import Any

from .graph import Step, Workflow, WorkflowError

POLICIES = ("fail", "continue", "skip_downstream")


def from_dict(data: dict[str, Any]) -> Workflow:
    if not isinstance(data, dict):
        raise WorkflowError("workflow must be an object")
    name = data.get("name")
    if not name or not isinstance(name, str):
        raise WorkflowError("workflow needs a name")

    raw_steps = data.get("steps")
    if not isinstance(raw_steps, list) or not raw_steps:
        raise WorkflowError("workflow needs a non-empty steps list")

    steps: list[Step] = []
    for item in raw_steps:
        if not isinstance(item, dict):
            raise WorkflowError("each step must be an object")
        sid = item.get("id")
        run = item.get("run")
        if not sid:
            raise WorkflowError("step missing id")
        if not run:
            raise WorkflowError(f"{sid}: step missing run target")
        raw_deps = item.get("deps") or []
        if isinstance(raw_deps, str):
            raw_deps = [raw_deps]
        if not isinstance(raw_deps, list):
            raise WorkflowError(f"{sid}: deps must be a list")
        policy = item.get("on_error", "fail")
        if policy not in POLICIES:
            raise WorkflowError(f"{sid}: on_error must be one of {POLICIES}")
        timeout = item.get("timeout")
        if timeout is not None and (not isinstance(timeout, (int, float)) or timeout <= 0):
            raise WorkflowError(f"{sid}: timeout must be a positive number")
        retries = item.get("retries", 0)
        if not isinstance(retries, int) or retries < 0:
            raise WorkflowError(f"{sid}: retries must be a non-negative integer")

        steps.append(
            Step(
                id=sid,
                run=run,
                deps=tuple(raw_deps),
                retries=retries,
                timeout=float(timeout) if timeout is not None else None,
                when=item.get("when"),
                on_error=policy,
            )
        )

    return Workflow(name=name, steps=steps)


STEP_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["id", "run"],
    "properties": {
        "id": {"type": "string", "minLength": 1},
        "run": {"type": "string", "description": "module:function"},
        "deps": {"type": "array", "items": {"type": "string"}},
        "retries": {"type": "integer", "minimum": 0},
        "timeout": {"type": "number", "exclusiveMinimum": 0},
        "when": {"type": "string"},
        "on_error": {"enum": list(POLICIES)},
    },
    "additionalProperties": False,
}
