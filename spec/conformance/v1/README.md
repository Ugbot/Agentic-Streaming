# Conformance fixtures, v1

A fixture is a workflow, a sequence of turns, and the expected normalized results. It is
data, not code, so the JVM and Python suites all read the same files. No runtime
ships its own copy and no runtime edits a fixture to make itself pass: a disagreement is
either a runtime bug or a spec change, and a spec change needs review from the JVM and
Python owners.

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
| `compensation-failure` | a failing compensation ends the turn `fatal`, the rest still run, and it never escapes the turn |
| `tool-timeout` | a tool that never answers fails its attempt at `timeout_ms`, and the attempts are numbered |
| `llm-transient-retry` | a transient model failure is retried inside the turn without repeating tools |
| `llm-transient-redelivery` | a transient failure past the budget does not spend the `turn_id`; a redelivery runs the turn |
| `duplicate-signal` | a resume signal redelivered after completion is a duplicate and rewrites nothing |
| `resume-transient-redelivery` | a resumed turn that fails transiently runs again on its redelivered signal |
| `timer-tool-failure` | a failing timer tool fails the turn that observed the deadline, and the next turn runs |
| `sequence-tool-turn`, `sequence-llm-turn`, `sequence-saga-turn` | the whole event sequence of each kind of turn, with the key, the user, a timestamp and metadata |
| `crash-at-every-event` | a crash around any event of a tool turn, then redelivery, ends as if nothing happened |
| `crash-call-idempotent`, `crash-call-non-idempotent`, `crash-call-read` | a crash before, during and after a call, for each effect class |
| `crash-between-attempts` | a crash between retries continues the attempt numbering |
| `unknown-outcome-applied`, `unknown-outcome-not-applied`, `unknown-outcome-failed` | each way a person can resolve an unknown outcome |
| `unknown-outcome-never-retries` | redelivery never retries an unknown outcome, before or after a restart |
| `unknown-outcome-bad-signal` | a signal that does not answer the question is refused and changes nothing |
| `unknown-outcome-twice` | a resolution is spent before its call, so a crash during that call is unknown again |
| `transient-after-non-idempotent` | a transient failure after a `non_idempotent` call does not repeat it |
| `llm-reused-after-crash` | a recorded model answer is reused by index, so a re-run takes the same path |
| `redelivery-mid-turn` | a partial turn is finished once, and everything after is a duplicate |
| `compensation-failed-event` | a failed undo is on the log, the others run, the turn ends fatal |
| `saga-resumes-forward`, `saga-resumes-compensation` | an incomplete saga carries on from the log, either way |
| `saga-undo-receives-forward` | an undo gets the args and result of the step it undoes |
| `saga-compensates-any-class` | a step failing on bad arguments compensates too |
| `signal-unknown-turn`, `signal-while-running`, `turn-during-suspension` | the refused and duplicate cells of the resume state machine |
| `crash-after-resumed`, `crash-after-resumed-turn-redelivered` | a crash past `turn_resumed` loses no approval |
| `resume-identity` | the original user keeps the turn; the signal sender is recorded apart |
| `saga-after-resume` | a resumed turn continues into its saga |
| `tool-http-5xx`, `tool-http-timeout` | a 5xx and a timeout are failed attempts over real HTTP |
| `non-idempotent-5xx`, `non-idempotent-4xx`, `non-idempotent-timeout` | what is unknown and what is a definite failure |
| `idempotency-key-on-the-wire` | the key arrives as a header, and is the one the spec defines |
| `long-lived-conversation` | a conversation outlives a restart under an idle TTL |
| `two-writers` | the same turn delivered twice at once is applied once |
| `allow-rail`, `path-guardrail`, `output-rail-after-verifier` | guardrails the two cores used to read differently |
| `argument-schema`, `router-static`, `missing-turn-id` | the rest of the differences between the cores |

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
   `crash_at` kills the delivery at the named point (see below), and `user_id` is the user the
   turn or signal arrives with.
4. Emit one normalized result per turn (`spec/v1/result.schema.json`).
5. Compare with `check_expectation` in `spec/tools/run_conformance.py`, or an equivalent
   implementation of the rules below.

## The `crash_at` verb

`crash_at` on a turn kills that delivery, as a process death would, and recovers the runtime from
what it keeps across a crash:

```yaml
- {conversation_id: c1, turn_id: t1, text: pay, crash_at: {event: tool_called, when: before}}
- {conversation_id: c1, turn_id: t1, text: pay, crash_at: {call: pay, when: during}}
```

`event` names an event type and `when` is `before` or `after` its append. `call` names a tool:
`before` it runs; `during`, once it has run and before its outcome is journaled; `after`, once the
outcome is journaled and before it is logged. `occurrence` picks the nth, default 1.

The binding raises something the turn cannot catch at that point, so no handler tidies up, and
records a result of status `crashed` with no events. Recovery then depends on the runtime:
reference, jvm-core and python keep what was appended, Pekko keeps what it persisted, and Flink
rolls back to a savepoint taken just before the delivery. What must survive every crash is the
outside world: the journal, the `counting` tools and the HTTP stub. The fixture's next turn is the
redelivery, and its expectation holds on every runtime, whatever the crash left behind.

Fixture tools that make this observable: `kind: counting` answers with the number of calls it has
seen for this conversation, or the number of distinct keys under `x-count-by: keys`, so a repeated
effect shows in the reply. An llm step with `x-count-calls: true` does the same for model calls. A
tool url of `{{http}}/...` points at a local stub the binding starts, serving `/ok`,
`/status/<code>`, `/hang` and `/echo` (`spec/tools/http_stub.py`).

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

The JVM bindings report through JUnit and leave their normalized results in
`target/conformance-results/<fixture id>.json` of their module. The matrix reads skips from the
JUnit XML and compares the results itself, so every runtime goes through
`run_conformance.check_expectation`. The ports of that comparator in `ConformanceHarness.java` and
`agentic/conformance.py` serve a plain `mvn test` or `pytest`; they are not the record.

## Comparison rules

- `status`, `path`, `reply`, and `error_class` are compared exactly when the expectation
  declares them. `reply_matches` is a regular expression searched against the reply.
- `tool_calls` is an exact sequence: tool id and order always, plus `index`, `attempt`,
  and `args` where the expectation names them. `failed: true` means the call recorded an
  error.
- `events` is the exact sequence of emitted event types: nothing extra, nothing missing. Fixtures
  from `32` on use it. `event_payloads` checks a subset of one event's payload, and `event_fields`
  checks every event carries `timestamp` or `metadata`.
- `events_include` is an ordered subsequence of the emitted event types, not the whole
  list, so a runtime may emit extra events. `events_exclude` must not appear at all.
- `status_any` accepts any of several statuses. It exists only where a log that rolled back and
  a log that did not honestly differ.
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
