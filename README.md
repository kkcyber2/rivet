# rivet

A workflow engine you can read in an afternoon. Real DAG validation, real retries, real timeouts, real failure semantics.

Most "workflow engines" are either a `for` loop with a JSON config or a 40,000-line platform. This is the middle: a small library that gets the semantics right, with tests that prove each one.

**Konain Sultan Khan (KK)** · [github.com/kkcyber2](https://github.com/kkcyber2) · kkdev9715@gmail.com

## Run it

```bash
python -m rivet.cli validate examples/ci.json
python -m rivet.cli run examples/ci.json --context '{"ref":"v1.4.0"}'
```

No dependencies. Python 3.10+.

```
valid: ci — 7 steps
   1. checkout
   2. install
   3. integration
   4. unit
   5. build
   6. deploy
   7. notify
```

```
  [ok  ] checkout (0.0s)
  [ok  ] install (0.0004s)
  [ok  ] integration (0.0s)
  [ok  ] unit (0.0s)
  [ok  ] build (0.0s)
  [ok  ] deploy (0.0101s)
  [ok  ] notify (0.0s)
run: ok
```

## The semantics that actually matter

**Cycle detection reports the cycle, not just its existence.** An iterative DFS returns the node path, rotated to start at the smallest id so the message is deterministic:

```
cycle detected: a -> b -> c
```

A naive implementation finds one cycle node and hangs, or reports "cycle found" leaving you to bisect the graph yourself.

**`on_error` has three real behaviours, and they are tested.**

| Policy | On failure |
| --- | --- |
| `fail` (default) | the run fails; every descendant is skipped |
| `continue` | the run carries on; **dependents still execute** |
| `skip_downstream` | only that branch is skipped; sibling branches run |

`continue` was a bug in the first version of this engine — the failed step was tolerated but the dependency check still vetoed its dependents, making the policy a no-op. The test `test_continue_policy_lets_dependents_run` is what caught it.

**Timeouts are enforced on a worker thread.** A step that hangs cannot hang the run, and a timeout is not retried — if it hung once it will hang again. Retries use bounded exponential backoff and report the attempt count.

**Conditions are a small expression language, not `eval`.** `when` supports `<step>`, `!<step>`, `<step> == <literal>` and `<step> != <literal>`. An expression referring to an unknown step raises rather than silently skipping — a typo in a condition should be loud.

**Outputs are keyed by step id**, not by function name. That is the only identifier the workflow author controls, so it is the stable key.

## Tests

```bash
python -m pytest tests/ -q
# 37 passed
```

Covers: duplicate ids, unknown deps, self-deps, negative retries, invalid policies, 2- and 3-node cycles, diamond graphs (no false positive), determinism, descendant walking, retries, timeouts, all three error policies, condition evaluation, listener ordering, and JSON serialisation of both successful and failed runs.

## Structure

```
rivet/graph.py    validation, cycle detection, topological order
rivet/engine.py   execution, retries, timeouts, error policies
rivet/spec.py     dict -> Workflow, with validation messages
rivet/cli.py      validate / run
examples/         a worked CI workflow with importable steps
tests/            the semantics above, pinned
```

## Licence

MIT — see [LICENSE](LICENSE).
