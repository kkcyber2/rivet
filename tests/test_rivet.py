"""Tests for the graph validator and the execution engine."""
from __future__ import annotations

import json
import time

import pytest

from rivet.engine import Engine, Status
from rivet.graph import CycleError, Step, Workflow, WorkflowError, descendants, topo_order
from rivet.spec import from_dict


def wf(*steps: Step, name: str = "test") -> Workflow:
    return Workflow(name=name, steps=list(steps))


# ----------------------------------------------------------------- graph tests
class TestValidation:
    def test_duplicate_id_refused(self):
        with pytest.raises(WorkflowError, match="duplicate step id"):
            topo_order(wf(Step("a", "x:f"), Step("a", "x:g")))

    def test_unknown_dependency_refused(self):
        with pytest.raises(WorkflowError, match="unknown step"):
            topo_order(wf(Step("a", "x:f", deps=("ghost",))))

    def test_self_dependency_refused(self):
        with pytest.raises(WorkflowError, match="depends on itself"):
            topo_order(wf(Step("a", "x:f", deps=("a",))))

    def test_negative_retries_refused(self):
        with pytest.raises(WorkflowError, match="retries"):
            topo_order(wf(Step("a", "x:f", retries=-1)))

    def test_bad_policy_refused(self):
        with pytest.raises(WorkflowError, match="on_error"):
            topo_order(wf(Step("a", "x:f", on_error="explode")))

    def test_empty_id_refused(self):
        with pytest.raises(WorkflowError, match="empty id"):
            topo_order(wf(Step("  ", "x:f")))


class TestCycles:
    def test_two_node_cycle_reports_the_nodes(self):
        w = wf(Step("a", "x:f", deps=("b",)), Step("b", "x:g", deps=("a",)))
        with pytest.raises(CycleError) as ei:
            topo_order(w)
        assert set(ei.value.cycle) == {"a", "b"}

    def test_three_node_cycle_is_fully_reported(self):
        w = wf(
            Step("a", "x:f", deps=("c",)),
            Step("b", "x:g", deps=("a",)),
            Step("c", "x:h", deps=("b",)),
        )
        with pytest.raises(CycleError) as ei:
            topo_order(w)
        assert set(ei.value.cycle) == {"a", "b", "c"}

    def test_cycle_message_reads_as_a_path(self):
        w = wf(Step("a", "x:f", deps=("b",)), Step("b", "x:g", deps=("a",)))
        with pytest.raises(CycleError) as ei:
            topo_order(w)
        assert "->" in str(ei.value)

    def test_valid_diamond_is_not_a_false_positive(self):
        # a -> b, a -> c, b+c -> d. No cycle, but every node has multiple paths.
        w = wf(
            Step("a", "x:f"),
            Step("b", "x:g", deps=("a",)),
            Step("c", "x:h", deps=("a",)),
            Step("d", "x:i", deps=("b", "c")),
        )
        assert topo_order(w) == ["a", "b", "c", "d"]


class TestOrder:
    def test_dependencies_precede_dependents(self):
        w = wf(
            Step("publish", "x:f", deps=("build",)),
            Step("build", "x:g", deps=("test",)),
            Step("test", "x:h"),
        )
        order = topo_order(w)
        assert order.index("test") < order.index("build") < order.index("publish")

    def test_order_is_deterministic_regardless_of_declaration_order(self):
        a = wf(Step("z", "x:f"), Step("a", "x:g"), Step("m", "x:h"))
        b = wf(Step("m", "x:h"), Step("z", "x:f"), Step("a", "x:g"))
        assert topo_order(a) == topo_order(b)

    def test_descendants_walks_the_whole_branch(self):
        w = wf(
            Step("a", "x:f"),
            Step("b", "x:g", deps=("a",)),
            Step("c", "x:h", deps=("b",)),
            Step("d", "x:i"),
        )
        assert descendants(w.by_id(), "a") == {"b", "c"}
        assert descendants(w.by_id(), "d") == set()


# ---------------------------------------------------------------- engine tests
def make(steps, fns, **kw):
    return Engine(functions=fns, sleep=lambda _s: None, **kw), wf(*steps)


class TestExecution:
    def test_happy_path_records_outputs(self):
        calls = []
        fns = {"t:a": lambda: calls.append("a") or 1, "t:b": lambda: calls.append("b") or 2}
        eng, w = make([Step("a", "t:a"), Step("b", "t:b", deps=("a",))], fns)
        run = eng.run(w)
        assert run.ok
        assert calls == ["a", "b"]
        assert run.results["b"].value == 2

    def test_outputs_are_available_to_downstream_steps(self):
        fns = {"t:a": lambda: 21, "t:b": lambda ctx: ctx["outputs"]["a"] * 2}
        eng, w = make([Step("a", "t:a"), Step("b", "t:b", deps=("a",))], fns)
        run = eng.run(w)
        assert run.results["b"].value == 42

    def test_failure_blocks_downstream(self):
        fns = {"t:a": lambda: (_ for _ in ()).throw(RuntimeError("boom")), "t:b": lambda: 1}
        eng, w = make([Step("a", "t:a"), Step("b", "t:b", deps=("a",))], fns)
        run = eng.run(w)
        assert run.results["a"].status is Status.FAILED
        assert run.results["b"].status is Status.SKIPPED
        assert not run.ok
        assert "boom" in run.results["a"].error

    def test_unrelated_branch_still_runs_after_failure(self):
        fns = {
            "t:a": lambda: (_ for _ in ()).throw(RuntimeError("x")),
            "t:b": lambda: 7,
        }
        eng, w = make([Step("a", "t:a"), Step("b", "t:b")], fns)
        run = eng.run(w)
        assert run.results["a"].status is Status.FAILED
        assert run.results["b"].status is Status.OK

    def test_retries_are_attempted_then_give_up(self):
        counter = {"n": 0}

        def flaky():
            counter["n"] += 1
            raise RuntimeError("nope")

        eng, w = make([Step("a", "t:a", retries=3)], {"t:a": flaky})
        run = eng.run(w)
        assert run.results["a"].attempts == 4          # 1 attempt + 3 retries
        assert counter["n"] == 4
        assert run.results["a"].status is Status.FAILED

    def test_retry_succeeds_on_second_attempt(self):
        counter = {"n": 0}

        def flaky():
            counter["n"] += 1
            if counter["n"] == 1:
                raise RuntimeError("first time")
            return "ok"

        eng, w = make([Step("a", "t:a", retries=2)], {"t:a": flaky})
        run = eng.run(w)
        assert run.results["a"].status is Status.OK
        assert run.results["a"].attempts == 2

    def test_timeout_is_enforced_and_not_retried(self):
        def slow():
            time.sleep(0.30)
            return "never"

        eng, w = make([Step("a", "t:a", timeout=0.05, retries=2)], {"t:a": slow})
        run = eng.run(w)
        assert run.results["a"].status is Status.TIMED_OUT
        assert run.results["a"].attempts == 1

    def test_continue_policy_lets_dependents_run(self):
        fns = {
            "t:a": lambda: (_ for _ in ()).throw(RuntimeError("x")),
            "t:b": lambda: 3,
        }
        eng, w = make([Step("a", "t:a", on_error="continue"), Step("b", "t:b", deps=("a",))], fns)
        run = eng.run(w)
        assert run.results["a"].status is Status.FAILED
        assert run.results["b"].status is Status.OK

    def test_skip_downstream_blocks_the_branch_but_not_others(self):
        fns = {
            "t:f": lambda: (_ for _ in ()).throw(RuntimeError("x")),
            "t:g": lambda: 1,
        }
        eng, w = make(
            [
                Step("a", "t:f", on_error="skip_downstream"),
                Step("b", "t:g", deps=("a",)),
                Step("c", "t:g", deps=("b",)),
                Step("d", "t:g"),
            ],
            fns,
        )
        run = eng.run(w)
        assert run.results["b"].status is Status.SKIPPED
        assert run.results["c"].status is Status.SKIPPED
        assert run.results["d"].status is Status.OK

    def test_when_false_skips_without_failing(self):
        fns = {"t:a": lambda: False, "t:b": lambda: "ran"}
        eng, w = make(
            [Step("a", "t:a"), Step("b", "t:b", deps=("a",), when="a")], fns
        )
        run = eng.run(w)
        assert run.results["a"].status is Status.OK
        assert run.results["b"].status is Status.SKIPPED
        assert run.results["b"].error == "condition false"

    def test_when_equality_condition(self):
        fns = {"t:a": lambda: "prod", "t:b": lambda: "deployed"}
        eng, w = make(
            [Step("a", "t:a"), Step("b", "t:b", deps=("a",), when='a == "prod"')], fns
        )
        assert eng.run(w).results["b"].status is Status.OK

    def test_when_equality_condition_false_skips(self):
        fns = {"t:a": lambda: "dev", "t:b": lambda: "deployed"}
        eng, w = make(
            [Step("a", "t:a"), Step("b", "t:b", deps=("a",), when='a == "prod"')], fns
        )
        assert eng.run(w).results["b"].status is Status.SKIPPED

    def test_condition_on_unknown_step_is_an_error_not_a_silent_skip(self):
        eng, w = make([Step("a", "t:a", when="ghost")], {"t:a": lambda: 1})
        with pytest.raises(ValueError, match="unknown step"):
            eng.run(w)

    def test_listener_sees_every_step_in_order(self):
        seen = []
        fns = {"t:a": lambda: 1, "t:b": lambda: 2}
        eng, w = make([Step("a", "t:a"), Step("b", "t:b", deps=("a",))], fns)
        eng._listener = lambda sid, res: seen.append(sid)
        eng.run(w)
        assert seen == ["a", "b"]

    def test_run_serialises_to_json(self):
        eng, w = make([Step("a", "t:a")], {"t:a": lambda: "v"})
        run = eng.run(w)
        blob = json.dumps(run.as_dict())
        assert json.loads(blob)["steps"][0]["value"] == "v"
        assert json.loads(blob)["ok"] is True

    def test_failed_run_serialises_as_not_ok(self):
        eng, w = make([Step("a", "t:a")], {"t:a": lambda: 1 / 0})
        payload = json.loads(json.dumps(eng.run(w).as_dict()))
        assert payload["ok"] is False
        assert "ZeroDivisionError" in payload["steps"][0]["error"]

    def test_cycle_is_refused_before_any_step_runs(self):
        called = []
        fns = {"t:a": lambda: called.append(1)}
        eng, w = make(
            [Step("a", "t:a", deps=("b",)), Step("b", "t:a", deps=("a",))], fns
        )
        with pytest.raises(CycleError):
            eng.run(w)
        assert called == []


class TestSpec:
    def test_minimal_spec_loads(self):
        w = from_dict({"name": "ci", "steps": [{"id": "t", "run": "a:b"}]})
        assert w.name == "ci"
        assert w.steps[0].deps == ()

    def test_string_dep_is_coerced_to_a_tuple(self):
        w = from_dict(
            {"name": "ci", "steps": [{"id": "t", "run": "a:b"}, {"id": "u", "run": "a:c", "deps": "t"}]}
        )
        assert w.steps[1].deps == ("t",)

    def test_missing_name_refused(self):
        with pytest.raises(WorkflowError, match="name"):
            from_dict({"steps": [{"id": "t", "run": "a:b"}]})

    def test_empty_steps_refused(self):
        with pytest.raises(WorkflowError, match="steps"):
            from_dict({"name": "x", "steps": []})

    def test_bad_timeout_refused(self):
        with pytest.raises(WorkflowError, match="timeout"):
            from_dict({"name": "x", "steps": [{"id": "t", "run": "a:b", "timeout": 0}]})

    def test_bad_retries_refused(self):
        with pytest.raises(WorkflowError, match="retries"):
            from_dict({"name": "x", "steps": [{"id": "t", "run": "a:b", "retries": -2}]})

    def test_unknown_policy_refused(self):
        with pytest.raises(WorkflowError, match="on_error"):
            from_dict({"name": "x", "steps": [{"id": "t", "run": "a:b", "on_error": "nope"}]})
