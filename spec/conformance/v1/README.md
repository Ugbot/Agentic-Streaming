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
| `llm-brain-scripted` | the stub LLM brain follows `llm.script`: structured tool calls, then the verbatim reply |
| `context-window` | `compaction: window` bounds the retained transcript to `max_items`; the log is untouched |
| `timer-fires` | a timer fires once when logical time reaches its deadline and invokes its tool with its payload |
| `event-time-timer` | an event-clock timer follows the watermark; late turns do not move it back |
| `cep-sequence` | a sequence pattern matches in order within its event-time window and fires `on_match` once |
| `timer-survives-restart` | a pending timer and the logical clock survive a restart; the timer fires once |
| `parallel-conversations` | concurrently delivered turns on different conversations are isolated |
| `path-verifier-override` | `paths.<name>.verifier` replaces `agent.verifier` for turns routed to that path |
| `path-verifier-fallback` | a path without a verifier is judged by `agent.verifier` |

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
   expecting the declared order. `restart_runtime` means: discard the runtime object and
   every in-memory cache, then rebuild from the binding's declared store. A binding may
   satisfy it with an in-process store that outlives the runtime object inside one test
   process. Passing the fixtures that require `durable_store` therefore proves that state
   is rebuilt from the log; it does not prove the store survives a process crash. Crash
   durability is claimed and tested per runtime (see the Runtimes section of the
   repository README for the list of those tests).
4. Emit one normalized result per turn (`spec/v1/result.schema.json`).
5. Compare with `check_expectation` in `spec/tools/run_conformance.py`, or an equivalent
   implementation of the rules below.

## Binding output contract

A binding that a cross-runtime runner (such as `spec/tools/conformance_matrix.py`) can
compare itself, rather than trusting the binding's ported comparator, writes one JSON
document listing every fixture in the directory, in file order:

```json
[
  {"id": "routing-keyword", "status": "passed", "results": [ <normalized result per turn> ]},
  {"id": "a2a-delegation", "status": "skipped", "skip_reason": "requires [a2a]"},
  {"id": "retry-tool", "status": "failed", "results": [ ... ], "problems": ["expect[1] (t2) ..."]}
]
```

- `status` is exactly one of `passed`, `failed`, `skipped`. A fixture is `skipped` only
  when a capability in its `requires` is not declared supported by the runtime; the
  `skip_reason` names those capabilities as `requires [a, b]`. A skip is never a pass.
- The fixture set is discovered, never enumerated: a binding lists every `*.yaml` in the
  directory and must not assert a fixture count. Its suite fails if the directory is empty
  or if any fixture was neither executed nor skipped with a reason naming the undeclared
  capabilities. Adding a fixture must never require editing a runtime's tests.
- `results` is present for `passed` and `failed` and holds one result per delivered turn,
  each valid against `result.schema.json`, so the runner can re-run the comparison rules
  below and validate the schema independently. `runtime_detail` may be included; it is
  ignored.
- `problems` is optional and informational; the runner's own comparison is authoritative.
- The document is printed to standard output on its own line between the marker lines
  `@@AGENTIC_CONFORMANCE@@` and `@@END@@`, so build tools may interleave their own output
  around it. Only one such block is printed per run.

A binding that only reports through its test framework (JUnit XML, pytest) is still
conformant; the matrix then records its comparison as made *inside the binding*.

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
  is compared, absolute numbers are not. The reason is the previous rule: `events_include`
  allows a runtime to emit events the fixture does not name (the reference, for example,
  appends `turn_failed` after a `rejected` or `unverified` turn, and a runtime that skips a
  capability emits none of that capability's events), and a runtime may number its log
  with its own persistence mechanism. Both make the absolute `sequence` of a named event
  differ between conformant runtimes, so a difference or apparent gap between two runtimes'
  numbering for the same event carries no information about correctness. Within one runtime the log must
  still be dense and monotonic per conversation (`spec/v1/primitives.md`, section 3); the
  fixtures check that indirectly through replay and idempotency, not by comparing numbers.
- Retry timing is not compared. Fixture `07-retry-tool` sets `initial_delay_ms: 0` and
  asserts the attempt sequence (`attempt: 1`, `attempt: 2`, ...) on `tool_calls` and on the
  `tool_called` and `tool_failed` events; whether a runtime slept between attempts is
  `runtime_detail`.
- The reference-only behaviours the fixtures depend on (the `anonymous` default user, the
  `[path]` reply prefix, the FNV-1a hashing embedder, cosine scoring with the `0.15`
  threshold, and guardrail stages) are stated normatively in `spec/v1/primitives.md`,
  section 9.

## Adding a fixture

1. Add the YAML file to `fixtures/`, numbered in reading order.
2. Declare the smallest `requires` set that is honestly needed.
3. Make it pass against the reference runtime, extending `reference_runtime.py` only for
   semantics the spec already states. If the spec does not state them, change the spec
   first.
4. Run `python spec/tools/validate_spec.py` and `pytest spec/tools/test_conformance.py`.
