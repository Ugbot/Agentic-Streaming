# The stream-stateful core — CEP, timers, windows, replay on every language

The project's thesis is *"an agent is a materialized view over an ordered log of events."* For a long
time the portable cores only did single-turn `submit(event) → TurnResult`; everything stream-native —
complex event processing, timers, windows — lived only on Flink. The **stream-stateful core** closes
that: a small set of modules, present and at **byte-for-byte parity** in all four cores
(`pyagentic` · `jagentic-core` · `goagentic` · `agentic-clj`), turns the event log into a first-class,
ordered, replayable stream and layers the stream-native capabilities on top.

Everything below is **model-free by default** and composes with the engine adapters: the portable
implementation is the default, and the SPI seam lets an engine inject its native primitive
(Flink timers/CEP, a Pekko scheduler + Pekko Streams, Temporal durable timers) without touching agent
code — exactly how the store SPIs already work.

## The modules

| Module | What it adds | Key types |
|--------|--------------|-----------|
| `stream` | Drive a **Channel** of events through the runtime as ordered-per-key turns; `submit` becomes one-shot sugar. An **observer** seam sees every event (CEP/windows/tracing plug in here). | `Channel`, `SeedChannel`, `QueueChannel`, `EventObserver`, `StreamRuntime` |
| `timers` | **Logical-time** timers — `schedule(id, fireAt, payload)` / `advanceTo(now)` (due in deadline order) / `cancel` / `nextDeadline`. In-memory default; **durable** via the `KeyedStateStore` SPI (survives restart). Powers SLAs, escalate-after-N, retries, CEP `within`. | `Timer`, `TimerService`, `InMemoryTimerService`, `DurableTimerService` |
| `windows` | Keyed **sliding** (the VelocityDetector), **tumbling**, and **session** windows with count/sum aggregates. | `SlidingWindow`, `TumblingWindow`, `SessionWindow` |
| `cep` | **Portable complex event processing** — a keyed NFA matcher over the stream: `begin / next [strict] / followedBy [relaxed] / within`, simple + iterative conditions, partial-match expiry. The same role as Flink CEP, on every core. | `Pattern`, `Condition`, `CepMatcher`, `Match`, `CepObserver` |
| `replay` | The event log is the source of truth; **replay** re-materializes state through a fresh (or new-version) graph; **replay-until** gives state as-of a point (time-travel). | `EventLog`, `Replayer` |
| `suspend` | **Human-in-the-loop**: a turn that needs approval suspends; resume (CQRS — just another command) replays it or denies; un-actioned suspensions escalate on timeout. | `Suspension`, `SuspensionService`, `HumanGate` |
| `trace` | A minimal **tracing SPI** — a span per turn and timer fire (path, ok, tool calls); no-op default, recording tracer, OpenTelemetry exporter opt-in. | `Tracer`, `Span`, `RecordingTracer` |

## The headline: CEP, off Flink

The canonical example — "**3 anomalies on one host within 5 minutes → open an incident**" — now runs
on every core, byte-identically (Java shown; the Python/Go/Clojure forms mirror it):

```java
Pattern incident = Pattern.begin("first",  Condition.any())
    .followedBy("second", Condition.any())
    .followedBy("third",  Condition.any())
    .within(Duration.ofMinutes(5).toMillis());

CepMatcher matcher = new CepMatcher(incident);
for (AnomalyEvent a : anomalies) {
  for (Match m : matcher.match(a.host(), a.ts(), a.toEvent())) {
    runtime.submit(incidentEvent(m));   // the match drives an agent turn
  }
}
```

Wire it onto the stream with `CepObserver` (key + timestamp extractors + an on-match handler) so the
matcher sees the live event stream and fires the agent only on a confirmed incident — the portable
equivalent of routing Flink CEP matches to a `PatternProcessFunction`.

## Declarative `cep:` in the pipeline

You rarely build matchers by hand — declare them. A `cep:` section in `pipeline.yaml`
([`examples/pipelines/incident.yaml`](../../examples/pipelines/incident.yaml)) compiles to wired
matchers whose matches fire actions, on every core:

```yaml
cep:
  - name: incident
    key: conversation_id          # conversation_id | metadata.<field>
    ts: metadata.ts               # metadata.<field> (else a per-rule arrival counter)
    within: 300000
    pattern:
      - { stage: first,  where: { text_contains: anomaly } }
      - { stage: second, where: { text_contains: anomaly }, contiguity: followedBy }
      - { stage: third,  where: { text_contains: anomaly }, contiguity: followedBy }
    on_match:
      kind: submit                # submit a derived event that routes through the graph (e.g. escalate)
      text: "incident: 3 anomalies on {key}"
      # or  kind: tool, tool: open_ticket, args: {...}   (call any registered tool — incl. an A2A peer / MCP tool)
```

`where`: `any` · `{text_contains: s|[..]}` · `{metadata_equals: {k: v}}` · `{metadata_gt: {k: n}}`.

**Run model.** The loader builds the rules and `PipelineSystem.submit` feeds every inbound event to
them *after* the turn; a match fires its action through the **inner** runtime. `submit` actions inject
a derived event tagged so CEP never re-matches it (no recursion); the stream path is a `StreamRuntime`
over the system, so CEP fires **exactly once** on either entry point. The same spec runs on Pekko
(`backend: pekko`) unchanged.

**On Flink it becomes a real job.** `FlinkPipelineRunner` reads the *same* `pipeline.yaml` and
assembles a Flink streaming job: source → native CEP (each `cep:` rule → a watermarked Flink
`Pattern` via `CepSpecTranslator`; a `submit` match emits a derived event unioned back into the agent
input) → `keyBy(conversation)` → the portable graph in a keyed operator → sink. So the declarative
agent — CEP and all — runs as the code-first DSL's equal, from YAML:
`mvn exec:java -Dexec.mainClass=org.agentic.flink.pipeline.FlinkPipelineRunner -Dexec.args="examples/pipelines/incident.yaml"`.

## How it composes with the engines

The portable impls are the **default and the cross-engine reference**; engines replace them behind the
SPIs where they have something native:

- **Flink** keeps its real CEP / keyed-state timers / windows (event-time + watermarks); the portable
  `cep` is the fallback and the parity reference.
- **Pekko** can back `TimerService` with its scheduler and the stream with Pekko Streams.
- **Temporal** can back timers with durable workflow timers.
- The `Tracer` SPI bridges to native tracing (OpenTelemetry) where present.

So **"CEP everywhere" means**: the portable engine by default, the native engine where one exists —
the agent definition is unchanged either way.

## Verification

Each module ships a cross-core parity test asserting byte-identical behaviour (the incident match,
timer fire order, window counts, replay reproduction, suspend/resume, span trees) the way the FNV
embedder and banking goldens already do. Run them with the usual per-core commands:
`pytest ports/pyagentic` · `mvn -f ports/jagentic-core/pom.xml test` · `go test ./...` (in `ports/go`)
· `clojure -X:test` (in `agentic-clj`).
