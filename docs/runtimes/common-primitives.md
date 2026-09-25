# Common primitives

What every `agentic/v1` runtime guarantees, and where the normative text lives. The normative
document is [spec/v1/primitives.md](../../spec/v1/primitives.md); this page is a map of it, not a
copy. When the two disagree, the spec wins. Each guarantee names the conformance fixtures under
`spec/conformance/v1/fixtures/` that check it, and the [matrix](../capabilities.md#fixtures) shows
which binding passed which fixture.

## The event log is the source of truth

A conversation is an ordered log of events with a closed type set and a dense, zero-based
`sequence`. Conversation state is a fold over that log, never stored on its own. A turn's
normalized result (`spec/v1/result.schema.json`) is a fold over that turn's events. Nouns and
the event set: [section 1](../../spec/v1/primitives.md#1-nouns). Fixtures: every fixture asserts
on the event subsequence; `replay-after-restart` and `memory-read-write` check the fold.

## `turn_id` is the idempotency key

A `turn_id` the runtime has already applied is answered with the recorded result and
`status: duplicate`. Nothing runs again: no brain, no tool, no guardrail. Tool calls are
idempotent per `(turn_id, call_index)`. [Section 4](../../spec/v1/primitives.md#4-idempotency).
Fixture: `duplicate-turn`.

## One writer per conversation

Per conversation, turns are processed one at a time in arrival order. Across conversations,
concurrency is unbounded. The runtime supplies the ordering (Flink `keyBy`, a Pekko mailbox
behind cluster sharding, a Clojure agent, a per-key lock in the pure Python engine); a runtime
that cannot guarantee it must declare `ordering` unsupported rather than approximate it.
[Section 3](../../spec/v1/primitives.md#3-ordering). Fixtures: `ordered-concurrent-turns`,
`parallel-conversations`.

## Effects are at-least-once

Exactly-once is a property of the log, not of side effects. Anything that leaves the system
(`publish`, `delegate`, tool calls with external effects) carries `(conversation_id, turn_id,
call_index)` so the receiver can deduplicate. [Section 4](../../spec/v1/primitives.md#4-idempotency)
and the retry rules in [section 5](../../spec/v1/primitives.md#5-errors-and-retries). Fixtures:
`tool-failure`, `retry-tool`, `saga-compensation`, `a2a-delegation`.

## Unknown events are preserved

A reducer ignores event types it does not know, and a runtime keeps them on replay, so a log
written by a newer minor version still replays on an older one.
[Section 1](../../spec/v1/primitives.md#1-nouns) (Event) and
[section 7](../../spec/v1/primitives.md#7-versioning). No shared fixture writes an unknown type;
the cores test it directly: `SpecPrimitivesTest.foldRejectsGapsAndIgnoresUnknownEventTypes`
(`ports/jagentic-core`), `test_file_event_log_survives_a_new_instance_and_preserves_unknown_types`
(`ports/pyagentic/tests/test_agentic_runtime.py`) and
`unknown-events-are-preserved-and-ignored-by-the-fold` (`agentic-clj/test/agentic/runtime_test.clj`).

## Timers and watermarks

A workflow declares `timers` per conversation; scheduling appends `timer_scheduled`, firing
appends `timer_fired` before the turn that observes it, and a timer fires once. A conversation's
watermark is the highest `event_time_ms` seen, reduced into `state.watermark_ms`; a late turn is
processed in arrival order and does not move the watermark. `checkpoint_recovery` means pending
timers and the logical clock survive a restart, rebuilt from the log.
[Clocks](../../spec/v1/primitives.md#clocks) and [Timers](../../spec/v1/primitives.md#timers).
Fixtures: `timer-fires`, `event-time-timer`, `timer-survives-restart`. In the current matrix only
the reference runtime passes them; every other binding skips them and therefore reports `timers`
and `checkpoint_recovery` as [unsupported](../capabilities.md#capabilities).

## CEP fold

A `cep` entry is a sequence pattern over one conversation's turns, evaluated after `routed` and
before the brain on every turn. Stages match turn text in declaration order with `next` or
`followedBy` contiguity, `within` bounds the event-time span of a match, matched turns are consumed
so matches never overlap, and `on_match.kind: tool` records a normal tool call on the completing
turn. [Sequence patterns](../../spec/v1/primitives.md#sequence-patterns-cep). Fixture: `cep-sequence`.

## Context window

`context.compaction: window` with `max_items: N` bounds the model-visible transcript to the most
recent `N` messages. The log is never compacted, so `state.turn_count` keeps counting while
`state.transcript_length` stays at most `N`. `moscow` and `max_tokens` are runtime specific and
not covered by v1 fixtures. [Context window](../../spec/v1/primitives.md#context-window). Fixture:
`context-window`.

## Verifiers

The verifier that judges a turn is the one on the path the turn was routed to, else
`agent.verifier`, else the default `prefix` verifier. A rejection appends `verification_failed`
and retries the brain up to `policies.verification.max_attempts`, then the turn ends
`unverified`. [Section 5](../../spec/v1/primitives.md#5-errors-and-retries). Fixtures:
`verification-failure`, `path-verifier-override`, `path-verifier-fallback`.

## Restart semantics

`restart_runtime` in a fixture means: drop every materialized view, keep only the log, and
continue. After a restart a redelivered `turn_id` is still a duplicate, a suspended turn still
resumes on its signal, and memory reads still see the earlier writes. Each runtime page says what
"restart" is for that runtime (a fresh `LocalRuntime` over the same log, a Flink stop-with-savepoint
and restore, a passivated Pekko entity). Fixtures: `replay-after-restart`, `suspend-resume`.

## Capability terminology

`supported`, `partial`, `unsupported` and `not_tested` are defined in
[section 6](../../spec/v1/primitives.md#6-capability-terminology), and
[capabilities.md](../capabilities.md) derives each cell from fixture outcomes, never from a
declaration. A runtime's own `capabilities()` method is what it claims; the matrix is what it
proved.

## The cores

Three cores implement these primitives once each, and the runtimes reuse them:

| Core | Language | Used by | Matrix column |
|---|---|---|---|
| `spec/tools/reference_runtime.py` | Python | the conformance runner, as the oracle | `reference` |
| `ports/jagentic-core` (`org.jagentic.core`) | Java 21 | [Flink](flink.md), [Pekko](pekko.md), [Python facade](python-facade.md), [PyFlink](pyflink.md) | `jvm-core` |
| `ports/pyagentic` (`agentic`) | Python | [pure Python](python.md) | `python` |

`agentic-clj` reimplements the primitives in Clojure over Datomic and has its own column,
`clojure`. The `jvm-core` binding runs `RoutedGraph` on `org.jagentic.core.LocalRuntime` with
in-memory stores; it is the floor for every JVM runtime's results.

<!-- matrix: reference, jvm-core -->
Derived from [capabilities.md](../capabilities.md) (run 2026-09-14T17:15:17+00:00, commit `eef9c63ea3ff`) by
`docs/tools/matrix_excerpt.py`; do not edit by hand. Every capability not listed below is
[supported](../capabilities.md#capabilities) for the binding, meaning every fixture that requires it passed.

Binding `reference`: 24 passed, 0 failed, 0 skipped.

Binding `jvm-core`: 24 passed, 0 failed, 0 skipped.

| Capability | reference | jvm-core |
|---|---|---|
<!-- /matrix -->
