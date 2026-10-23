"""Workflow graph: validation, topological order, and cycle reporting.

A workflow engine's first job is refusing an invalid graph with a message that
tells you which edge is wrong. Most toy implementations either accept anything
and fail at run time, or report a cycle without saying where it is.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


class WorkflowError(ValueError):
    """The graph itself is invalid."""


class CycleError(WorkflowError):
    """The graph contains a cycle; ``cycle`` holds the offending node ids."""

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        super().__init__("cycle detected: " + " -> ".join(cycle))


@dataclass(frozen=True)
class Step:
    """One node. ``deps`` are ids that must succeed before this runs."""

    id: str
    run: str                       # dotted path, resolved at execution time
    deps: tuple[str, ...] = ()
    retries: int = 0
    timeout: float | None = None
    when: str | None = None        # optional expression over upstream outputs
    on_error: str = "fail"         # fail | continue | skip_downstream


@dataclass
class Workflow:
    name: str
    steps: list[Step] = field(default_factory=list)

    def by_id(self) -> dict[str, Step]:
        return {s.id: s for s in self.steps}


def _validate_ids(steps: Iterable[Step]) -> dict[str, Step]:
    seen: dict[str, Step] = {}
    for s in steps:
        if not s.id or not s.id.strip():
            raise WorkflowError("a step has an empty id")
        if s.id in seen:
            raise WorkflowError(f"duplicate step id: {s.id}")
        if s.retries < 0:
            raise WorkflowError(f"{s.id}: retries cannot be negative")
        if s.on_error not in ("fail", "continue", "skip_downstream"):
            raise WorkflowError(f"{s.id}: unknown on_error policy {s.on_error!r}")
        seen[s.id] = s
    return seen


def _validate_deps(steps: dict[str, Step]) -> None:
    for s in steps.values():
        for d in s.deps:
            if d not in steps:
                raise WorkflowError(f"{s.id} depends on unknown step {d!r}")
            if d == s.id:
                raise WorkflowError(f"{s.id} depends on itself")


def find_cycle(steps: dict[str, Step]) -> list[str] | None:
    """Return the node ids forming a cycle, or ``None``.

    Iterative DFS, so a pathological graph cannot blow the Python stack. The
    returned path is rotated so the smallest id comes first, which makes the
    error deterministic regardless of traversal order.
    """
    WHITE, GREY, BLACK = 0, 1, 2
    color = {sid: WHITE for sid in steps}
    stack_path: list[str] = []

    for root in sorted(steps):
        if color[root] != WHITE:
            continue
        # frame = (node, iterator over deps)
        work: list[tuple[str, list[str]]] = [(root, list(steps[root].deps))]
        color[root] = GREY
        stack_path.append(root)

        while work:
            node, deps = work[-1]
            if deps:
                nxt = deps.pop()
                if color[nxt] == GREY:
                    # back edge: slice the current path at the repeat
                    i = stack_path.index(nxt)
                    return stack_path[i:]
                if color[nxt] == WHITE:
                    color[nxt] = GREY
                    stack_path.append(nxt)
                    work.append((nxt, list(steps[nxt].deps)))
            else:
                color[node] = BLACK
                work.pop()
                stack_path.pop()

    return None


def topo_order(wf: Workflow) -> list[str]:
    """Kahn's algorithm with a deterministic tie-break.

    Sorting the ready set means the same workflow always produces the same
    execution order, which is what makes a run reproducible.
    """
    steps = _validate_ids(wf.steps)
    _validate_deps(steps)

    cycle = find_cycle(steps)
    if cycle:
        raise CycleError(cycle)

    indeg = {sid: 0 for sid in steps}
    for s in steps.values():
        for _ in s.deps:
            indeg[s.id] += 1

    ready = sorted(sid for sid, n in indeg.items() if n == 0)
    order: list[str] = []
    while ready:
        sid = ready.pop(0)
        order.append(sid)
        # decrement dependents
        for other in steps.values():
            if sid in other.deps:
                indeg[other.id] -= 1
                if indeg[other.id] == 0:
                    ready.append(other.id)
        ready.sort()

    if len(order) != len(steps):        # pragma: no cover - guarded by find_cycle
        raise CycleError(sorted(steps))
    return order


def descendants(steps: dict[str, Step], start: str) -> set[str]:
    """Every step reachable downstream of ``start``, excluding ``start``."""
    out: set[str] = set()
    frontier = list(_direct_dependents(steps, start))
    while frontier:
        sid = frontier.pop()
        if sid in out:
            continue
        out.add(sid)
        frontier.extend(_direct_dependents(steps, sid))
    return out


def _direct_dependents(steps: dict[str, Step], sid: str) -> set[str]:
    return {s.id for s in steps.values() if sid in s.deps}
