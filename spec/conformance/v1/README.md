# Conformance fixtures, v1

A fixture is a workflow, a sequence of turns, and the expected normalized results. It is
data, not code, so the JVM, Clojure, and Python suites all read the same files. No runtime
ships its own copy and no runtime edits a fixture to make itself pass: a disagreement is
either a runtime bug or a spec change, and a spec change needs review from the JVM,
Clojure, and Python owners.

## Fixtures

| Fixture | Proves |
|---|---|
| `routing-keyword` | the keyword router selects the declared path |
| `routing-default` | unmatched text falls through to `router.default` |
| `tool-invocation` | a trigger produces one structured tool call with its arguments |
| `tool-failure` | a failing tool fails the turn with error class `tool`, no silent empty result |
| `guardrail-rejection` | an input guardrail rejects before routing, status `rejected` |
| `verification-failure` | the verifier exhausts its attempts and the turn ends `unverified` |
| `retry-tool` | retries are attempt-numbered and the successful attempt completes the turn |
| `duplicate-turn` | a redelivered `turn_id` returns the recorded result, status `duplicate`, no re-execution |
| `ordered-concurrent-turns` | concurrent turns on one conversation apply serially in arrival order |
| `memory-read-write` | the transcript is per conversation and visible to the next turn |
| `replay-after-restart` | state is rebuilt from the log, and replay does not re-execute tools |
| `suspend-resume` | a suspended turn survives a restart and completes on its signal |
| `saga-compensation` | compensations execute in reverse order and are recorded as events |
| `a2a-delegation` | a peer agent is callable as a tool and the delegation is recorded |
| `retrieval` | retrieval over the hashing embedder is deterministic and ordered |

Run them against the reference runtime:

```
python spec/tools/run_conformance.py
python spec/tools/run_conformance.py duplicate-turn   # one fixture
```

## How a runtime binding consumes a fixture

1. Read the fixture, resolve `workflow` or `workflow_ref`, and build the workflow.
2. If the runtime declares any of the fixture's `requires` capabilities as `unsupported`,
   record `skipped`. A skip is never a pass, and it is what the capability matrix reports.
3. Deliver each turn in order. Honour `restart_runtime` by restarting or recovering the
   runtime, `advance_time_ms` by advancing logical time, `signal` by resuming the named
   turn, and `concurrent_with` by delivering the listed turns concurrently while still
   expecting the declared order.
4. Emit one normalized result per turn (`spec/v1/result.schema.json`).
5. Compare with `check_expectation` in `spec/tools/run_conformance.py`, or an equivalent
   implementation of the rules below.

## Comparison rules

- `status`, `path`, `reply`, and `error_class` are compared exactly when the expectation
  declares them. `reply_matches` is a regular expression searched against the reply.
- `tool_calls` is an exact sequence: tool id and order always, plus `index`, `attempt`,
  and `args` where the expectation names them. `failed: true` means the call recorded an
  error.
- `events_include` is an ordered subsequence of the emitted event types, not the whole
  list, so a runtime may emit extra events. `events_exclude` must not appear at all.
- `state_includes` is a subset check over the reduced state.
- Everything under `runtime_detail` is excluded: checkpoint ids, actor addresses,
  offsets, wall-clock timings, engine metrics. Two runtimes may differ there and still
  be conformant.
- Wall-clock timestamps and `sequence` values are not compared across runtimes; ordering
  is compared, absolute numbers are not.

## Adding a fixture

1. Add the YAML file to `fixtures/`, numbered in reading order.
2. Declare the smallest `requires` set that is honestly needed.
3. Make it pass against the reference runtime, extending `reference_runtime.py` only for
   semantics the spec already states. If the spec does not state them, change the spec
   first.
4. Run `python spec/tools/validate_spec.py` and `pytest spec/tools/test_conformance.py`.
