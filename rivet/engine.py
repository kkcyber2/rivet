"""Execution engine.

Runs a validated DAG. Consequential behaviours a workflow engine is judged on
and that this actually implements:

* retries with exponential backoff, bounded
* per-step timeout enforced on a worker thread (so a hung step cannot hang the run)
* ``on_error`` policies, including skipping only the affected downstream branch
* a condition (``when``) that can skip a step without failing its dependents
* a run record you can serialise, replay and diff
"""
from __future__ import annotations

import importlib
import time
import threading
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Callable

from .graph import Step, Workflow, descendants, topo_order, CycleError


class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    OK = "ok"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMED_OUT = "timed_out"


@dataclass
class StepResult:
    id: str
    status: Status
    value: Any = None
    error: str | None = None
    attempts: int = 0
    elapsed: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "value": self.value,
            "error": self.error,
            "attempts": self.attempts,
            "elapsed": round(self.elapsed, 4),
        }


@dataclass
class Run:
    workflow: str
    results: dict[str, StepResult] = field(default_factory=dict)
    started: float = 0.0
    finished: float = 0.0

    @property
    def ok(self) -> bool:
        return all(r.status in (Status.OK, Status.SKIPPED) for r in self.results.values())

    def as_dict(self) -> dict[str, Any]:
        return {
            "workflow": self.workflow,
            "ok": self.ok,
            "elapsed": round(self.finished - self.started, 4),
            "steps": [self.results[s].as_dict() for s in self.results],
        }


class _Timeout(Exception):
    pass


class Engine:
    """Resolves step functions from dotted names and runs the graph.

    ``functions`` lets a caller inject implementations directly, which is how the
    tests run without touching a module registry.
    """

    def __init__(
        self,
        functions: dict[str, Callable[..., Any]] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        listener: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self._functions = functions or {}
        self._sleep = sleep
        self._listener = listener

    # ------------------------------------------------------------------ resolve
    def resolve(self, dotted: str) -> Callable[..., Any]:
        if dotted in self._functions:
            return self._functions[dotted]
        if ":" not in dotted:
            raise LookupError(f"cannot resolve step target {dotted!r} (expected 'module:function')")
        mod_name, _, attr = dotted.partition(":")
        module = importlib.import_module(mod_name)
        try:
            return getattr(module, attr)
        except AttributeError as exc:
            raise LookupError(f"{mod_name} has no attribute {attr!r}") from exc

    # ---------------------------------------------------------------------- run
    def run(self, wf: Workflow, context: dict[str, Any] | None = None) -> Run:
        order = topo_order(wf)              # raises CycleError / WorkflowError
        steps = wf.by_id()
        run = Run(workflow=wf.name, started=time.time())
        ctx = dict(context or {})
        ctx.setdefault("outputs", {})

        blocked: set[str] = set()
        # Steps that failed but were explicitly told to let the run continue. Their
        # dependents are allowed to execute: that is the whole meaning of
        # on_error=continue, and leaving them blocked makes the policy a no-op.
        tolerated: set[str] = set()

        for sid in order:
            step = steps[sid]

            if sid in blocked:
                run.results[sid] = StepResult(sid, Status.SKIPPED, error="upstream failed")
                self._emit(run, sid)
                continue

            # A dependency that did not produce a value means this step cannot run,
            # unless that dependency failed under an on_error=continue policy.
            missing = [
                d for d in step.deps
                if d not in tolerated
                and (
                    run.results.get(d) is None
                    or run.results[d].status not in (Status.OK, Status.SKIPPED)
                )
            ]
            if missing:
                run.results[sid] = StepResult(sid, Status.SKIPPED, error=f"deps not ok: {missing}")
                self._emit(run, sid)
                continue

            if step.when is not None and not self._eval_cond(step.when, ctx, run):
                run.results[sid] = StepResult(sid, Status.SKIPPED, error="condition false")
                self._emit(run, sid)
                continue

            result = self._run_step(step, ctx)
            run.results[sid] = result
            if result.status is Status.OK:
                ctx["outputs"][sid] = result.value
                ctx[sid] = result.value
            self._emit(run, sid)

            if result.status is not Status.OK:
                if step.on_error == "continue":
                    tolerated.add(sid)
                    continue
                if step.on_error == "skip_downstream":
                    blocked |= descendants(steps, sid)
                    continue
                # fail: everything downstream is unreachable
                blocked |= descendants(steps, sid)

        run.finished = time.time()
        return run

    # ------------------------------------------------------------------ helpers
    def _emit(self, run: Run, sid: str) -> None:
        if self._listener:
            self._listener(sid, run.results[sid].as_dict())

    def _eval_cond(self, expr: str, ctx: dict[str, Any], run: Run) -> bool:
        """Conditions are a genuinely small expression language, not ``eval``.

        Supported: ``<step_id>``, ``!<step_id>``, ``<step_id> == <literal>``,
        ``<step_id> != <literal>``. Anything else raises, rather than silently
        evaluating attacker-supplied Python.
        """
        expr = expr.strip()
        if not expr:
            raise ValueError("empty condition")

        for op in ("==", "!="):
            if op in expr:
                left, right = expr.split(op, 1)
                left = left.strip()
                raw = left.lstrip("!")
                if raw not in run.results:
                    return False if raw not in ctx else False
                val = ctx.get("outputs", {}).get(raw)
                lit = right.strip()
                target: Any
                if lit in ("True", "true"):
                    target = True
                elif lit in ("False", "false"):
                    target = False
                elif lit in ("None", "null"):
                    target = None
                elif (lit.startswith('"') and lit.endswith('"')) or (
                    lit.startswith("'") and lit.endswith("'")
                ):
                    target = lit[1:-1]
                else:
                    try:
                        target = int(lit)
                    except ValueError:
                        try:
                            target = float(lit)
                        except ValueError:
                            target = lit
                eq = val == target
                return eq if op == "==" else not eq

        negate = expr.startswith("!")
        name = expr[1:].strip() if negate else expr
        if name not in run.results and name not in ctx:
            raise ValueError(f"condition refers to unknown step {name!r}")
        truthy = bool(ctx.get("outputs", {}).get(name, ctx.get(name)))
        return (not truthy) if negate else truthy

    def _run_step(self, step: Step, ctx: dict[str, Any]) -> StepResult:
        fn = self.resolve(step.run)
        attempts = 0
        started = time.time()
        last: str | None = None

        for attempt in range(step.retries + 1):
            attempts = attempt + 1
            try:
                value = self._call_with_timeout(fn, step.timeout, ctx, step.id)
                return StepResult(
                    step.id, Status.OK, value=value, attempts=attempts,
                    elapsed=time.time() - started,
                )
            except _Timeout:
                last = f"timed out after {step.timeout}s"
                # A timeout is not retried: if the step hung once it will hang again.
                return StepResult(
                    step.id, Status.TIMED_OUT, error=last, attempts=attempts,
                    elapsed=time.time() - started,
                )
            except Exception as exc:                    # noqa: BLE001 - recorded, not swallowed
                last = f"{type(exc).__name__}: {exc}"
                if attempt < step.retries:
                    self._sleep(min(0.05 * (2 ** attempt), 1.0))

        return StepResult(
            step.id, Status.FAILED, error=last, attempts=attempts,
            elapsed=time.time() - started,
        )

    def _call_with_timeout(
        self, fn: Callable[..., Any], timeout: float | None, ctx: dict[str, Any], sid: str
    ) -> Any:
        if timeout is None:
            return _invoke(fn, ctx, sid)

        box: dict[str, Any] = {}

        def target() -> None:
            try:
                box["value"] = _invoke(fn, ctx, sid)
            except BaseException as exc:                # noqa: BLE001 - forwarded to the caller
                box["error"] = exc

        t = threading.Thread(target=target, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive():
            raise _Timeout()
        if "error" in box:
            raise box["error"]
        return box.get("value")


def _invoke(fn: Callable[..., Any], ctx: dict[str, Any], sid: str) -> Any:
    """Call a step with whatever arity it declares.

    Zero-arg functions are the common case; a function that accepts one argument
    gets the context. This keeps step definitions readable.
    """
    import inspect

    try:
        sig = inspect.signature(fn)
        params = [
            p for p in sig.parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
    except (TypeError, ValueError):
        params = []
    return fn(ctx, sid) if len(params) >= 2 else (fn(ctx) if params else fn())
