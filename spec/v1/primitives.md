# Agentic Streaming primitive specification, v1

Status: draft-1. Version: `agentic/v1`.

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
| `Path` | a named branch: brain, tools, guardrails, verifier | path name | pure config |
| `Brain` | produces a reply text for a turn | path-scoped | pure or model-backed |
| `Tool` | a named, side-effecting function with structured arguments | `tool.id` | registry-scoped |
| `ToolCall` | one structured invocation of a tool within a turn | `(turn_id, index)` | recorded as an event |
| `Guardrail` | admits or rejects a turn's input or output | ordinal in the list | pure |
| `Verifier` | accepts or rejects a candidate reply | path or workflow scoped | pure |
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

The capability matrix is generated from test results (Phase 5.2). A capability may not be
called native in documentation without a test that proves it.

Capability ids in v1: `routing`, `rule_brain`, `llm_brain`, `tools`, `structured_tool_args`,
`guardrails`, `verifier`, `ordering`, `idempotency`, `retry`, `memory`, `retrieval`,
`context_window`, `replay`, `suspend_resume`, `timers`, `saga`, `a2a`, `cep`, `event_time`,
`checkpoint_recovery`, `parallelism`, `durable_store`.

## 7. Versioning

- The spec version is `agentic/v1` and appears as `spec_version` in every workflow document.
- Additive fields are minor changes; a runtime must accept unknown fields under documented
  extension points (`x-*` keys and `runtime:` blocks) and must reject unknown fields elsewhere.
- Removing or re-meaning a field requires `agentic/v2`.
- A runtime declares the spec versions it accepts. Loading a newer major version is a
  `validation` error, not a best-effort parse.
