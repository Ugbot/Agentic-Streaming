# Agentic Streaming primitive specification, v1

Status: frozen for conformance fixtures 01 to 24. Version: `agentic/v1`. See section 7 for what
may still change under this version. Section 10 (effects and recovery) was added under the
additive rules of section 7.

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
`delegated`, `llm_called`, `tool_intent`, `compensation_failed`, `unknown_outcome`, `tool_refused`.

`llm_called`, `tool_intent`, `compensation_failed` and `unknown_outcome` were added under section 7
and are defined in section 10. `tool_refused` is defined in section 11.

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
| `suspended` | the turn awaits an external signal, a timer, or a person deciding an unknown outcome |
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
| `resume` | `(conversation_id, turn_id, signal) -> TurnResult` | continues the suspended turn (section 10) |
| `record_or_get` | `(key, intent, call) -> outcome` | the effect journal: runs `call` at most once per recorded outcome (section 10) |
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
- A resume signal redelivered after its turn reached a terminal status is a `duplicate` of that
  turn: nothing is appended and the recorded result stands.
- A turn that ended `failed` with class `transient` has not been applied, so its `turn_id` is
  not spent. A redelivery runs the turn again rather than answering `duplicate`, and does not
  append a second `turn_received`, so it is not counted twice. A redelivered resume signal runs
  the brain of that turn again. Tools the first run called are called again, under the next
  free indices, so `(turn_id, index)` still names one call. A run that reached a
  `non_idempotent` tool is the exception: it is replayed from the journal instead, because that
  tool is never called twice (section 10). Every other failed turn is applied.
- Tool calls are idempotent per `(turn_id, call_index)`. A retried turn must not double-apply
  a tool whose result is already recorded. The effect journal of section 10 is what makes this
  true, including after a rollback that loses the log's own record of the call.
- Every tool call and every effect that leaves the system (`publish`, `delegate`) carries a
  deterministic `Idempotency-Key` derived from `(conversation_id, turn_id, call_index)`, so the
  receiver can deduplicate (section 10).
- State is exactly-once. Effects are at-least-once under a stable key. Anything that cannot take
  a key is an explicit human decision: `effect: non_idempotent` tools are never called twice by
  the runtime, and an unknown outcome suspends the turn for a person (section 10).

## 5. Errors and retries

Error classes:

| Class | Examples | Default handling |
|---|---|---|
| `validation` | spec rejected, unknown tool, bad arguments | fail the turn, no retry |
| `guardrail` | input or output blocked | terminal status `rejected`, no retry |
| `verification` | verifier rejected the reply | retry the brain up to `policies.verification.max_attempts`, then `unverified` |
| `tool` | tool raised or timed out | retry per `policies.retry`, then `turn_failed` unless the path declares a fallback |
| `policy` | a tool refused by the tool policy (section 11) | fail the turn, no retry; a saga compensates |
| `transient` | model unreachable, timed out, rate limited or a server error; store or network failure | retry per `policies.retry`, then `turn_failed`; a redelivery runs the turn again (section 4) |
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

The turn boundary. Handling a turn never raises to the runtime for anything the turn did. A
failure no other rule covers ends the turn `failed`, with `turn_failed` of class `fatal` as
its last event, and a redelivery is a `duplicate` of that failure like any other applied turn.
A runtime must not fail, restart, or stop for a per-conversation error: other conversations
keep running. It surfaces turns ending `fatal` somewhere an operator can act on them (on Flink
the `turn-failures` side output). The one exception is a log that cannot be read or written,
which is an infrastructure failure: the turn cannot record its end there, so the runtime fails
as a whole and recovers.

Failed compensation. A compensation that fails, after `policies.retry`, is recorded as
`tool_failed` followed by `compensation_failed`, and does not stop the others: every remaining
compensation still runs, in reverse order. `on_tool_error: continue` does not apply to
compensations. `compensation_completed` is written only when every compensation succeeded. When
any failed it is absent, and the turn ends `failed` with class `fatal` whatever the failed step's
class was, because the saga is partly applied and needs repair by hand.

Time limits. Every tool attempt and every model call has a limit. A tool's `timeout_ms` bounds
one attempt, default 10000; a peer's bounds one delegation, default 30000; `llm.timeout_ms`
bounds one model call and `embeddings.timeout_ms` one embedding call, default 60000 for both. A
tool that reaches its limit fails that attempt like any
other tool failure, so it is recorded and retried under `policies.retry` and then has class
`tool`. A timeout means the answer did not arrive, not that nothing happened: the effect may
have landed. Tool kind `hanging` never answers and exists so that a fixture can prove the limit
is enforced.

A timer's tool that fails ends the turn that observed the deadline as `failed` with class
`tool`, before `turn_received`. `timer_fired` stays on the log, so the timer does not fire
again.

A model call is retried on its own under `policies.retry`, so a transient failure does not
repeat the tools the model already called this turn. A failure the provider will repeat, such
as a refused request or a rejected key, is `fatal` and is not retried. A stub script step with
`x-fail-attempts: n` fails its first `n` model calls transiently, counted across turns, so a
fixture can exercise this.

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

The capability matrix is generated from fixture outcomes by
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
`checkpoint_recovery`, `parallelism`, `durable_store`, `crash_recovery`, `effect_journal`,
`tool_policy`, `principal`.

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
  (`Turn.user_id` default). `turn_received` records it. A resumed turn keeps the user of the
  original turn, read from the log; whoever sent the signal is recorded separately, as
  `signal_sender` on `turn_resumed` (section 10). Fixture `03-tool-invocation` asserts the
  resulting tool argument.
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
  zero (`embed`). This is the embedder the Python and JVM cores share at byte parity;
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
  `routed` event is emitted for a rejected turn (`_check_guardrails`, `_handle`). A
  rail runs its `allow` patterns first, and every one must match or the turn is rejected; then
  its `deny` patterns, and any match rejects. A rail named in a path's `guardrails` list applies
  to that path only, after `routed`; every other rail is global. `output` and `both` rails run on
  the drafted reply after the verifier accepts it and before `memory_written`, and a match ends
  the turn `rejected`. Fixtures `allow-rail`, `path-guardrail` and `output-rail-after-verifier`
  prove this.
- Tool arguments. When a tool declares `parameters`, every call's arguments are validated against
  that JSON Schema before the call is journaled or given an index. Failure is class `validation`:
  nothing reaches the tool. Fixture `argument-schema` proves it.
- HTTP tools. A 2xx answer is the result. Anything else, and a timeout, is a failed attempt.
  Fixture `tool-http-5xx` proves it.
- Router kinds. `keyword` and `static` are implemented by every runtime. `static` always selects
  `router.default`.
- Sequence numbers. `sequence` is the position of the event in the conversation log, so it is
  dense per conversation within one runtime (`_append`). Absolute values are not compared
  across runtimes; see the comparison rules in `spec/conformance/v1/README.md` for why.
- Retry observability. Section 5: attempts are observable through `attempt` numbers on
  `tool_called` and `tool_failed` events and on `tool_calls`; delays are not observed.

## 10. Effects and recovery

This section is the contract for the sentence in section 4. It holds on every runtime, including
one whose log can roll back behind the outside world, as Flink's keyed state does.

### What is and is not guaranteed

| Thing | Guarantee |
|---|---|
| Conversation state and log | Exactly-once. A recovered turn leaves the same log an uninterrupted run would |
| Model calls | The recorded reply is reused by index. A crash during a model call asks the model again |
| `effect: read` tools | Recorded results are reused. No recorded result means the read runs again |
| `effect: idempotent` tools | At-least-once under a stable `Idempotency-Key`. No recorded result means the call is made again, under the same key. Exactly one effect only if the tool honours the key |
| `effect: non_idempotent` tools | The runtime never makes a second call. A call with no recorded result is `unknown_outcome` and waits for a person |

**The limit, stated plainly.** A crash during a call to a `non_idempotent` tool leaves the outcome
unknown. The journal cannot tell whether the effect landed, nothing in this specification can, and
no runtime may guess. The call is neither retried nor treated as failed until a person says what
happened. A journal gives reuse of recorded outcomes. It does not make an outside system
transactional.

### Tool effect classes

A tool declares `effect: read`, `idempotent` or `non_idempotent`. A tool that declares nothing is
`non_idempotent`: it is never called twice, it gets one attempt, and a call with no recorded result
is `unknown_outcome`, suspended for a person. The safe guess about a tool nobody has described is
that a second call would do harm, so a tool that is safe to repeat says `read` or `idempotent`, and
a runtime whose log rolls back refuses an undeclared tool without a journal that outlives the
rollback. The same goes for an `a2a` peer, whose client retries are switched off under
`non_idempotent`, and for an `mcp` server, whose `effect` covers all its tools.

`policies.retry` applies to `read` and `idempotent` tools. A `non_idempotent` tool gets one attempt
per call, whatever the policy says.

### The idempotency key

Every tool call carries a key derived from recorded input alone, so a re-run regenerates it:

```
Idempotency-Key = lowercase hex SHA-256 of
                  UTF-8(conversation_id) 0x00 UTF-8(turn_id) 0x00 ASCII(decimal index)
```

Every attempt at one call carries the same key; a compensation has its own index and so its own
key. An `http` or `agent` tool sends it as the header `Idempotency-Key`. A tool bound in process
receives it with the call's identity `(conversation_id, turn_id, index)`. MCP tools receive it in
the request's `_meta`.

### The journal

`EffectJournal` is a keyed store outside the runtime's own state, so it survives whatever rolls the
log back. One entry per call:

```
key      : (conversation_id, turn_id, kind, index)    # kind is tool or llm
intent   : see below
effect   : read | idempotent | non_idempotent
state    : pending | succeeded | failed | unknown
attempts : [{attempt, error}]                          # the failed attempts, in order
result   : the recorded result, or the reply for llm
error    : the final error, when failed
```

`intent` says what the call was: the tool id and a SHA-256 of the workflow's `agent.id` and the
arguments as canonical JSON, with map keys sorted at any depth. For a model call it is a SHA-256 of
`agent.id`, the brain's instructions and the turn's text. The transcript is left out, because a log
that kept part of a turn shows more of it than the first run saw. A hash no longer than this does not
do: a 32-bit one lets two different calls pass for one.

`record_or_get(key, intent, call)` is the only way a tool or model is reached:

1. If an entry with a final state exists, return it. Nothing is called.
2. If the entry is `pending`, the previous run stopped without an answer. A `read` or `idempotent`
   call continues at its next attempt, under the same key. A `non_idempotent` call becomes
   `unknown`.
3. If there is no entry, write `pending` durably **before** calling. A store that cannot be written
   is an infrastructure failure, as for the log (section 5): the call is not made.
4. Call, then write the outcome durably before anything else happens.

An entry is written whole, so a store only needs a keyed upsert: a table with a primary key or a
compacted topic. Writes are synchronous and bounded in time: a store that stops answering is a store
that cannot be written. An entry whose intent differs from the call now being made means the turn
did not replay deterministically, for instance because the workflow changed under it. Nothing is
called and the turn ends `failed` with class `fatal`. A run that would complete with a recorded call
still ahead of its last index ends the same way, so a recorded effect does not vanish behind a
success. If either the entry or the current declaration says `non_idempotent`, the call is treated
as `non_idempotent`: what a run recorded outranks what a later version of the workflow declares. A
call the log shows as begun and the journal does not hold is `pending`, never a first call.

Model calls use the same entry shape with `kind: llm`, counted by their own index within the turn.
They are recorded when they answer; no `pending` is written, because asking again costs money but
changes nothing. A recorded reply whose intent differs is the same divergence as for a tool. Retrieval is not journaled, since section 2 makes it deterministic for a fixed
index.

A journal is declared as `stores.effects`. Implementations: `memory` (a process-wide store for
tests and local use), `file` (Python), `postgres` and `kafka` (JVM; a compacted topic). A runtime
whose log rolls back must not run a `non_idempotent` tool without a journal that outlives the
rollback: it rejects the workflow at build time. `on_unavailable: degrade` on `stores.effects` falls
back to memory only for a workflow with no `non_idempotent` tool; with one, the journal failing to
open stops the workflow. Retention must outlast the longest rollback or
redelivery a deployment allows, and an entry dropped sooner re-opens that turn to repeat calls.

### New events

| Event | Written | Payload |
|---|---|---|
| `tool_intent` | once per call, before the first attempt | `tool`, `index`, `args`, `effect`, `idempotency_key` |
| `llm_called` | after each model answer, recorded or reused | `index`, `model` where known, `tool` and `args` or `text` |
| `unknown_outcome` | when a call's outcome cannot be known | `tool`, `index`, `args`, `idempotency_key`, `reason` |
| `compensation_failed` | after the `tool_failed` of an undo whose budget is spent | `tool`, `index`, `step`, `error` |

`tool_called` and `delegated` also carry `result`.

### Replay of a turn

A run of a turn starts from the top every time: after a crash, after a rollback, and on a resume. It
re-derives each step and consults the journal, so what was recorded is reused and the sequence of
calls is the same. Appending is at-most-once per `(turn_id, type, index, attempt)`, or per
occurrence for events without an index: an event already on the log is reused, not appended again.
A log that lost the turn gets every event again; a log that kept part of it gets the rest. Both end
up with the log an uninterrupted run would have written.

A turn's `events` in a result are its events from the start, or from its last `turn_resumed`.

Timers are evaluated against the log as it stood before the turn's first event, so a re-run gives
the same tool indices. A turn that ended `failed` with class `transient` is not replayed: section 4
says what its redelivery does, and its calls take the next free indices, unless the run reached a
`non_idempotent` tool, in which case it is replayed like a crashed turn.

### Sagas

A compensation receives `{"forward_args": <args>, "forward_result": <result>}` of the step it
undoes. Any failure of a step compensates, whatever its class: a tool failure, bad arguments or an
unknown tool. A step's or an undo's `unknown_outcome` suspends the turn, and the saga carries on
when a person resolves it. An incomplete saga resumes from the log: redelivery replays the steps
from the journal and carries on where the crash left it, forward or backward, and no completed step
or undo runs twice. A failed compensation is terminal (section 5).

### The resume state machine

Every delivery of a turn or signal falls in exactly one cell. Each state comes from the fold of the
log; `running` is any turn with events and no terminal status, which only a crash can leave.

| State of the `turn_id` | A turn arrives | A signal arrives |
|---|---|---|
| absent | run it | **refused** |
| `running`, no `turn_resumed` yet | replay it | **refused** |
| `running`, after `turn_resumed` | replay it | replay it |
| `suspended` | `duplicate`; nothing runs | **resume** |
| `suspended`, `until: unknown_outcome` | `duplicate`; nothing runs | **resolve**, or **refused** when the signal has no valid `resolution` |
| terminal, class `transient` | run again (section 4) | run again (section 4) |
| terminal | `duplicate` | `duplicate` |

A signal is checked before any of this: section 11 says who may send one.

**Refused** means status `failed`, class `validation`, with **nothing appended**, so the `turn_id`
stays unspent. A signal that overtakes its turn must not burn it. The same applies to a turn
delivered with an empty or missing `turn_id`, which section 4 forbids: it cannot be deduplicated, so
it is refused rather than given an invented id.

**Resume** appends `turn_resumed {turn_id, signal, signal_sender}` once, then replays the turn past
its suspension. The turn runs as the user and principal of `turn_received`. `signal_sender` is who
the signal came from, recorded apart with its `signal_principal` (section 11), and has no effect on
what runs. After a resume the turn continues as it would have:
into the saga if the workflow declares one, otherwise into the brain.

**Resolve** is a resume whose signal answers an unknown outcome:

| `signal.resolution` | Meaning | Effect |
|---|---|---|
| `applied` | it happened; `signal.result` is what it returned | the entry becomes `succeeded` with that result, and the turn carries on |
| `not_applied` | it did not happen; call it again | a person's decision: the entry is cleared, and the call runs under the same key |
| `failed` | it failed | the entry becomes `failed` and the turn fails as if the tool had; a saga compensates |

An unknown outcome is never retried without one of these. Redelivering the turn answers `duplicate`.
A resolution is spent durably before the call it allows, so a crash during that call is unknown
again, and a redelivered signal changes nothing about an entry that is no longer `unknown`.

A `non_idempotent` call also becomes `unknown` when its one attempt ends without a definite answer:
a timeout, a lost connection, or an HTTP 5xx, which may have landed. A 4xx or a tool that raises is
a definite failure. The turn appends `unknown_outcome`, then `turn_suspended` with `until:
unknown_outcome` and the `tool` and `index` in question.

## 11. Authorisation

Section 10 says a call happens once. This section says whether it may happen at all.

### The tool boundary

Every tool call passes one check, before it is validated, journaled or given an index, whatever asked
for it. The check denies by default.

| Origin | What it may call |
|---|---|
| `brain` (rule, llm or custom) | the routed path's `tools`, plus the tools of the path's `skills` |
| `saga`, `compensation` | the same: a saga runs on the routed path |
| `timer`, `cep` | `policies.unattended_tools`: they run outside any path, so no path covers them |

`tools: []` means none, and a path with no `tools` has none. A `tool_triggers` entry, a saga step, a
timer's `tool` and a pattern's `on_match.tool` each say what to call; none of them grants it.

A refused call appends `tool_refused {tool, origin, path, reason}` and nothing else: no index, no
`tool_intent`, no journal entry. The turn then ends `failed` with class `policy`. A refused tool is
not offered back to a model, which sees only the tools the policy grants its path, so a model
that asks for another gets no second try in that turn. In a saga a refused step is a failed step: what
ran before it compensates. A refused undo is a failed undo: `compensation_failed`, and the turn ends
`fatal`. `on_tool_error: continue` does not apply to a refusal.

### The principal

A `Principal` is who a turn or a signal is authenticated as: `subject`, and optionally `tenant`,
`roles` and `issuer`. Whatever puts it on the event vouches for it: a gateway from a verified token,
a topic only trusted producers can write to, or the program embedding the runtime. A runtime
cannot tell a true one from a false one, so the boundary is whoever may deliver events.

Beside a principal, `user_id` is the principal's `subject`, whatever the caller claimed, so a name in
a request body never reaches the log, a tool's arguments or a brain. Without a principal, `user_id` is
a label and nothing more.

`turn_received`, `tool_intent` and `tool_refused` carry `principal` when there is one. A run of a turn
that is already on the log takes its principal from `turn_received`, never from the delivery, so a
resume keeps the identity of the turn.

### Who may answer a suspended turn

`policies.approvers` names `subjects` and `roles`. A signal is honoured only from a principal with one
of them, and of the conversation's tenant. Deny by default: without the block nobody can resume a turn
or settle an unknown outcome, and a signal with a `user_id` and no principal is nobody.

A signal for a turn that is on the log is checked before the resume state machine, so the same rule
covers a resume, a resolution and a redelivered signal. Refusal appends `signal_refused {turn_id,
signal_sender, signal_principal, reason}` and nothing else. The result is status `failed`, class
`policy`. The turn stays as it was and the journal is not touched, so the approver can still answer.
A signal for a turn that is not on the log is refused as section 10 says, with nothing appended, so
it cannot grow a log.

A timer the runtime set for itself resumes the turn without an approver: it is not a signal from
outside, and no event can claim to be one.

### Tenancy on the log

A conversation belongs to the tenant of its first `turn_received`. A turn with any other tenant, or
with no principal, is refused with class `policy` and nothing appended. A conversation started with no
tenant has no such check.

### Egress

An HTTP tool, an HTTP peer and a model call check their URL against the operator's egress policy
before anything is sent: `http` and `https` only, no credentials in the URL, and no private, loopback,
link-local or metadata address unless the operator allows it. The settings belong to the runtime's
deployment, not to the document. A refused attempt is a definite failure with the error `<tool>
failed: blocked by egress policy`, so a `non_idempotent` tool is not left unknown. Redirects are not
followed. A binding allows its own local stub and nothing else, so a fixture can prove the refusals.

