# Agentic Streaming primitive specification, v1

Status: frozen for conformance fixtures 01 to 24. Version: `agentic/v1`. See section 7 for what
may still change under this version.

This is the normative vocabulary every runtime implements. A runtime is conformant when it
executes the workflow IR (`workflow.schema.json`) and produces the normalized turn result
(`result.schema.json`) required by the conformance fixtures under `spec/conformance/v1/`.

Names here are the contract. Language bindings adapt spelling (`conversationId` on the JVM,
`conversation_id` in Python and in the IR, `:conversation-id` in Clojure) but not meaning.

## 1. Nouns

| Noun | Definition | Identity | Lifetime |
|---|---|---|---|
| `Agent` | a named unit that produces a reply for one turn on one path | `agent.id` | spec-scoped, immutable |
| `Workflow` | the whole declarative unit: router, paths, tools, stores, policies | spec document | versioned by `spec_version` |
| `Conversation` | the keyed entity a workflow runs against | `conversation_id` | durable, unbounded |
| `Turn` | one inbound request and its processing to a terminal status | `turn_id` | single, ordered |
| `Event` | an immutable record appended to a conversation log | `(conversation_id, sequence)` | durable |
| `State` | the value obtained by reducing a conversation's events | derived | rebuilt by `replay` |
| `Router` | maps an inbound turn to exactly one path | `router.kind` | pure |
| `Path` | a named branch: brain, tools, guardrails, and optionally its own verifier | path name | pure config |
| `Brain` | produces a reply text for a turn | path-scoped | pure or model-backed |
| `Tool` | a named, side-effecting function with structured arguments | `tool.id` | registry-scoped |
| `ToolCall` | one structured invocation of a tool within a turn | `(turn_id, index)` | recorded as an event |
| `Guardrail` | admits or rejects a turn's input or output | ordinal in the list | pure |
| `Verifier` | accepts or rejects a candidate reply; a path's own takes precedence over the workflow's (section 5) | path or workflow scoped | pure |
| `Memory` | the conversation transcript plus keyed scalars | `conversation_id` | durable |
| `Context` | the model-visible window assembled from memory and retrieval | per turn | ephemeral |
| `Store` | a durable backing for conversations, keyed state, facts, vectors | connection link | external |
| `Channel` | an inbound or outbound transport that carries events | channel id | runtime-scoped |
| `Timer` | a scheduled callback bound to a conversation | `(conversation_id, timer_id)` | durable where supported |
| `Saga` | an ordered list of steps with compensations | `(conversation_id, turn_id)` | per turn |
| `Peer` | another agent addressable as a tool (A2A) | peer name | external |
| `Runtime` | the engine that supplies ordering and durability | runtime name | deployment |

### Event

An event is the atomic durable record. The log is the source of truth; `State` is derived.

```
Event {
  conversation_id : string        # partition and state key
  turn_id         : string        # idempotency key, stable across redelivery
  sequence        : int           # 0-based, dense, per conversation
  type            : EventType
  payload         : object        # type-specific, JSON-serializable
  timestamp       : int           # epoch millis, engine clock
  metadata        : map<string,string>
}
```

`EventType` is a closed set in v1:

`turn_received`, `guardrail_rejected`, `routed`, `brain_started`, `tool_called`,
`tool_failed`, `retrieved`, `reply_drafted`, `verification_failed`, `memory_written`,
`turn_completed`, `turn_failed`, `turn_suspended`, `turn_resumed`, `timer_scheduled`,
`timer_fired`, `compensation_started`, `compensation_step`, `compensation_completed`,
`delegated`.

Unknown event types must be preserved on replay and ignored by reducers, so a runtime on
an older spec version can still rebuild state written by a newer one.

### Turn result

A turn ends in exactly one terminal status:

| Status | Meaning |
|---|---|
| `completed` | a reply was produced and verified |
| `rejected` | an input or output guardrail blocked the turn |
| `unverified` | the verifier rejected the reply after all retries |
| `failed` | an unrecoverable error; the log records `turn_failed` |
| `suspended` | the turn awaits an external signal or timer |
| `duplicate` | the `turn_id` was already applied; the prior result is returned verbatim |

## 2. Verbs

| Verb | Signature (conceptual) | Contract |
|---|---|---|
| `route` | `(turn, state) -> path` | pure, total, deterministic; falls back to `router.default` |
| `handle` | `(turn, context) -> TurnResult` | the whole turn; ordered per conversation |
| `invoke` | `(tool_id, args) -> result` | structured args, never string-parsed; may fail |
| `observe` | `(event) -> unit` | listener hook; must not alter results |
| `append` | `(conversation_id, event) -> sequence` | atomic, ordered, at-most-once per `(turn_id, type, index)` |
| `reduce` | `(state, event) -> state` | pure, total, deterministic |
| `replay` | `(conversation_id, up_to?) -> state` | reduce over the log; no external side effects |
| `retrieve` | `(query, k) -> passages` | deterministic for a fixed index and embedder |
| `verify` | `(reply, context) -> ok \| reject(reason)` | pure |
| `retry` | `(attempt, policy) -> delay \| give_up` | bounded by `policies.retry` |
| `compensate` | `(saga, failed_step) -> unit` | executes compensations in reverse order |
| `suspend` | `(turn, reason) -> suspended` | state is durable across the pause |
| `resume` | `(conversation_id, turn_id, signal) -> TurnResult` | continues the suspended turn |
| `delegate` | `(peer, turn) -> result` | a peer agent invoked as a tool |
| `publish` | `(channel, message) -> unit` | outbound transport |
| `subscribe` | `(channel) -> stream<event>` | inbound transport |

## 3. Ordering

- Per conversation, turns are processed one at a time, in arrival order, by a single writer.
- Concurrency across conversations is unbounded and unordered.
- Ordering is supplied by the runtime (Flink `keyBy`, Pekko mailbox plus cluster sharding,
  Kafka partition, Temporal workflow execution, an in-process per-key lock locally). A runtime
  that cannot guarantee it must declare `ordering` as `unsupported` in the capability matrix,
  not approximate it.
- `sequence` is dense and monotonic per conversation. A gap is a bug, not a tolerated state.

## 4. Idempotency

- `turn_id` is the idempotency key and is mandatory on every inbound turn. A runtime that
  receives a `turn_id` it has already applied must return the recorded result with status
  `duplicate`, and must not re-run the brain, tools, or guardrails.
- Tool calls are idempotent per `(turn_id, call_index)`. A retried turn must not double-apply
  a tool whose result is already recorded.
- Effects that leave the system (`publish`, `delegate`) carry `(conversation_id, turn_id,
  call_index)` so the receiver can deduplicate.
- Exactly-once is a property of state, not of side effects: the conversation log is the
  source of truth, external effects are at-least-once with a deduplication key.

## 5. Errors and retries

Error classes:

| Class | Examples | Default handling |
|---|---|---|
| `validation` | spec rejected, unknown tool, bad arguments | fail the turn, no retry |
| `guardrail` | input or output blocked | terminal status `rejected`, no retry |
| `verification` | verifier rejected the reply | retry the brain up to `policies.verification.max_attempts`, then `unverified` |
| `tool` | tool raised or timed out | retry per `policies.retry`, then `turn_failed` unless the path declares a fallback |
| `transient` | store or network failure | retry per `policies.retry` |
| `fatal` | serialization failure, corrupt state | fail the turn and surface; never silently degrade |

Retry policy:

```yaml
policies:
  retry:
    kind: exponential      # none | fixed | exponential
    max_attempts: 3
    initial_delay_ms: 100
    multiplier: 2.0
    max_delay_ms: 5000
    jitter: false          # deterministic by default so fixtures are reproducible
```

Retries never re-append `turn_received`. Each attempt appends its own `tool_called` or
`tool_failed` events with an incrementing `attempt` in the payload.

Verifier resolution. A verifier may be declared at `agent.verifier` and on any path at
`agent.paths.<name>.verifier`. The verifier that judges a turn is the one on the path the
turn was routed to; when that path declares none, `agent.verifier`; when neither is declared,
the default `prefix` verifier of section 9. Resolution happens once per turn, after `routed`,
and the same verifier judges every draft the brain produces for that turn, including drafts
after a `verification_failed` and the brain run of a resumed turn. A path-level `kind: none`
is a declaration: it disables verification on that path even when `agent.verifier` would
reject. Which level supplied the verifier is not observable in the event log: the events,
statuses, and error class are those of any verification, so a reducer that does not know
about path-level verifiers reads the log unchanged.

Retry timing is observable only through the event log, never through wall-clock
measurement. What a runtime must expose is the sequence of attempts: attempt `n` appears as
its own `tool_called` (and, on failure, `tool_failed`) event with `attempt: n`, in order,
and the normalized `tool_calls` list carries the same `attempt` numbers. Whether the runtime
actually sleeps for `initial_delay_ms` between attempts is not part of the contract and is not
compared; the fixtures set `initial_delay_ms: 0` and `jitter: false`, and the reference
runtime does not sleep at all. A runtime that honours the delay in production and skips it
under test is conformant as long as the attempt numbering is identical.

Degradation is explicit: no component may silently substitute an in-memory store for a
configured durable store. A configured store that cannot be reached is a `fatal` error at
build time unless the spec sets `stores.<name>.on_unavailable: degrade`.

## 6. Capability terminology

Every runtime declares each capability as exactly one of:

| Value | Meaning |
|---|---|
| `supported` | implemented natively and proven by a passing conformance or runtime test |
| `partial` | implemented with a stated restriction, which must be named in the declaration |
| `unsupported` | not available; the runtime rejects specs that require it, loudly |
| `not_tested` | implemented but unproven; may not be described as supported in documentation |

The capability matrix (`docs/capabilities.md`) is generated from fixture outcomes by
`spec/tools/conformance_matrix.py`. A capability may not be called native in documentation
without a test that proves it.

`durable_store` needs one more sentence. `durable_store: supported` means the three fixtures
that require it (`replay-after-restart`, `suspend-resume`, `timer-survives-restart`) passed
with the store the binding declares, and the binding honoured `restart_runtime` by discarding
its runtime object and rebuilding from the store. Most bindings run that against an
in-process store inside a single test process. It proves that state is rebuilt from the log
rather than cached, not that the log survives a process crash, a node loss, or a cluster
restart. Crash durability is a runtime claim, proven per runtime by tests that restart a real
persistence mechanism (a Flink savepoint, a Pekko journal, a Datomic database, a file store);
the list of those tests is kept in the repository README under Runtimes.

Capability ids in v1: `routing`, `rule_brain`, `llm_brain`, `tools`, `structured_tool_args`,
`guardrails`, `verifier`, `ordering`, `idempotency`, `retry`, `memory`, `retrieval`,
`context_window`, `replay`, `suspend_resume`, `timers`, `saga`, `a2a`, `cep`, `event_time`,
`checkpoint_recovery`, `parallelism`, `durable_store`.

## 7. Versioning

- The spec version is `agentic/v1` and appears as `spec_version` in every workflow document.
- `agentic/v1` is frozen with respect to the conformance fixtures `01` to `24` under
  `spec/conformance/v1/fixtures`. The behaviour those fixtures assert, the event types and
  payload fields they observe, and the normalized result shape in `result.schema.json` do not
  change under this version. A change that would make a currently passing runtime fail one of
  them, or that would change what a fixture asserts, is a breaking change.
- Additive changes are allowed under `agentic/v1`: new optional fields, new capability ids,
  new event types, and new fixtures numbered `25` and up. They are governed by the
  unknown-field rule: a runtime must accept unknown fields under the documented extension
  points (`x-*` keys and `runtime:` blocks), must preserve unknown event types on replay and
  ignore them in reducers (section 1), and must reject unknown fields elsewhere as a
  `validation` error. A new fixture may only require capabilities a runtime can declare
  `unsupported` and skip; it may not silently widen an existing capability.
- The path-level verifier (`agent.paths.<name>.verifier`, section 5) was added under
  `agentic/v1` as such a change: the field was already optional in `workflow.schema.json`,
  its absence keeps the previous behaviour exactly, it adds no event type or payload key, and
  fixtures `23` and `24` prove it under the existing `verifier` capability. A runtime that
  declares `verifier` supported must honour it; it may not ignore the field or reject it.
- Removing or re-meaning a field, an event type, or a payload key, changing a fixture's
  expectation, or changing the normative defaults in section 9 requires `agentic/v2`, a new
  directory `spec/v2`, and a new fixture set; v1 fixtures keep running against v1 runtimes.
- A runtime declares the spec versions it accepts. Loading a newer major version is a
  `validation` error, not a best-effort parse.
- `spec/tools/validate_spec.py` and `python -m pytest spec/tools` are the gate: they check the
  schemas, every fixture, and the reference runtime against each other, and a change that
  makes them fail is not an additive change.

## 8. Time, patterns, context, and the scripted brain

These semantics are what the `timers`, `event_time`, `checkpoint_recovery`, `cep`,
`context_window`, `llm_brain`, and `parallelism` capability ids mean. Each is proven by a
conformance fixture; a runtime that does not implement one declares it `unsupported` and
skips the fixture.

### Clocks

A runtime has two clocks, both observable only through turns:

- **Processing time** is the runtime's logical clock. It never runs backwards and is part
  of recovered state: a restart does not reset it. Fixtures advance it with
  `advance_time_ms`; a runtime without a controllable logical clock declares `timers`
  `unsupported`.
- **Event time** is carried by the turn as `metadata.event_time_ms` (an integer in decimal
  string form, since metadata values are strings). A conversation's **watermark** is the
  highest event time seen on its turns; it is recorded in the `turn_received` payload as
  `event_time_ms` and reduced into `state.watermark_ms`. A late turn (event time below the
  watermark) is processed in arrival order like any other and does not move the watermark
  back.

### Timers

`timers` are declared on the workflow and scheduled per conversation on its first turn,
`after_ms` past the chosen clock's reading at that moment. Scheduling appends
`timer_scheduled {timer_id, clock, due_ms}` after that turn's `turn_received`. A timer
fires on the first later turn of the conversation delivered when its clock reads at or
past `due_ms`: before that turn's `turn_received`, the runtime appends
`timer_fired {timer_id, due_ms}` and, if the timer names a `tool`, invokes it with
`payload` as arguments, recorded as that turn's first tool call. Due timers fire in
`(due_ms, timer_id)` order. A timer fires once; `state.fired_timers` lists the ids that
have fired, in order.

`checkpoint_recovery` means pending timers and the logical clock survive a restart: they
are rebuilt from `timer_scheduled` and `timer_fired` events (or an equivalent checkpoint),
never re-scheduled and never fired twice.

### Sequence patterns (CEP)

A `cep` entry is a sequence pattern over one conversation's turns, evaluated after `routed`
and before the brain on every turn. Stages match in declaration order against turn text
(`where.text_contains`, case-insensitive). A stage's `contiguity` says how it follows the
previous one: `next` (default) requires the immediately following turn, and a
non-matching turn drops the partial match; `followedBy` skips non-matching turns.
`within` bounds the event-time span between the first and last matched turn, read from
the metadata key named by `ts`; a partial match that exceeds it is dropped and the current
turn may start a new one. A match completes on the turn satisfying the last stage; its
turns are consumed, so matches never overlap. `on_match.kind: tool` invokes the tool on
the completing turn with arguments `{"pattern": <name>, "key": <conversation_id>}`,
recorded as a normal tool call. `on_match.kind: submit` injects a derived turn and is
outside the v1 fixtures.

### Context window

`context.compaction: window` with `max_items: N` bounds the model-visible transcript to
the most recent `N` messages. The log is never compacted; only the retained transcript is,
so `state.turn_count` keeps counting while `state.transcript_length` reports the retained
message count, at most `N`. `compaction: none` retains everything. `moscow` and
`max_tokens` are runtime-specific and not covered by v1 fixtures.

### Scripted LLM brain

A path with `brain: llm` under `llm.provider: stub` is driven by `llm.script`, replayed
from the top on every turn: each `{tool, args}` step is one structured tool call with
exactly those arguments, and the first `{text}` step is the reply, verbatim, with no
`[path]` prefix. This is the portable form of the LLM control loop (call tools, then
answer); a real provider replaces the script, not the loop.

### Parallelism

`parallelism` means turns on different conversations may run concurrently and every
conversation's state, transcript, timers, and tool calls are its own. Section 3's ordering
guarantee is per conversation only. The fixture proves isolation under concurrent
delivery; a runtime that processes keys serially may still pass it and must say so in its
capability declaration.

## 9. Reference defaults that are normative

The fixtures assert on replies, tool arguments, and retrieval results that depend on details
first encoded only in `spec/tools/reference_runtime.py`. Those details are part of the
contract; a runtime that differs on any of them fails the corresponding fixture. Each item
names the code it was checked against.

- Default user. A turn without `user_id` has `user_id` equal to the string `anonymous`
  (`Turn.user_id` default). A resumed turn is rebuilt from the suspended record and also
  carries `anonymous`, whatever the original turn said. Fixture `03-tool-invocation` asserts
  the resulting tool argument.
- Rule-brain tool arguments. When a `tool_triggers` keyword matches (case-insensitive
  substring on the turn text, first match in declaration order), the rule brain invokes the
  tool with exactly one argument, `{"user": <user_id>}`, and replies
  `[<path>] <tool_id> returned <result>` where `<result>` is the tool result rendered as text
  (`_draft`).
- Reply prefix. Every reply the rule brain produces starts with `[<path>] ` where `<path>` is
  the routed path name: the tool reply above, the retrieval reply `[<path>] <passage text>`,
  the fallback `[<path>] I can help with <path> questions. You said: "<turn text>"`, and the
  saga reply `[<path>] saga completed`. The default `prefix` verifier accepts a reply if and
  only if it starts with `[`. The scripted LLM brain (section 8) is the one exception: its
  `{text}` step is the reply verbatim, with no prefix.
- Hashing embedder. `embeddings.provider: hashing` is FNV-1a, 32-bit, over each token's
  UTF-8 bytes, with offset basis `0x811C9DC5` and prime `0x01000193`. Tokens are the maximal
  runs matching `[a-z0-9]+` after lower-casing the text. Each token adds `1.0` to slot
  `fnv1a_32(token) mod dim`; the vector is then L2-normalized, and an all-zero vector stays
  zero (`embed`). This is the embedder the Python, JVM, and Clojure cores share at byte parity;
  the fixture `15-retrieval` depends on it producing the same order on every runtime.
- Retrieval scoring and threshold. Candidates are scored by cosine similarity between the
  query vector and each passage vector (`cosine`; zero if either norm is zero), sorted by
  descending score and then ascending passage id, and cut to `retrieval.top_k`. The
  `retrieved` event carries the ids in that order and reduces into `state.last_retrieved_ids`.
  The top passage becomes the reply only if its score is strictly greater than the path's
  `threshold`, default `0.15`; otherwise the fallback reply is used, and the `retrieved`
  event is still emitted.
- Guardrail stages. A guardrail's `stage` is `input` (default), `output`, or `both`. Input
  guardrails run on the turn text after `turn_received` and before routing; a match appends
  `guardrail_rejected` and ends the turn `rejected` with error class `guardrail`, so no
  `routed` event is emitted for a rejected turn (`_check_guardrails`, `_handle`). The reference
  runtime evaluates `input` and `both` rails at the input stage only and skips `output`
  rails entirely; no v1 fixture exercises an output-stage rail. A runtime that implements
  output rails runs them on the drafted reply after the verifier accepts it and before
  `memory_written`, and a match ends the turn `rejected`; that placement is stated here so
  implementations agree, but it is not conformance tested in v1 and a runtime may not describe
  output guardrails as `supported` on the strength of the shared fixtures.
- Sequence numbers. `sequence` is the position of the event in the conversation log, so it is
  dense per conversation within one runtime (`_append`). Absolute values are not compared
  across runtimes; see the comparison rules in `spec/conformance/v1/README.md` for why.
- Retry observability. Section 5: attempts are observable through `attempt` numbers on
  `tool_called` and `tool_failed` events and on `tool_calls`; delays are not observed.
