# Incident-agent walkthrough

> **Flink-runtime showcase** — exercises Flink-only capabilities (**CEP** pattern matching + keyed
> state). Not the portable baseline; for the agent that runs unchanged on every runtime see
> [the banking agent on every runtime](banking-everywhere.md).

> Source: `src/main/java/org/agentic/flink/example/incident/IncidentAgentExample.java`
> Inline README: `src/main/java/org/agentic/flink/example/incident/README.md`

## Why this shape

The argument for combining anomaly detection, CEP, and LLM is **don't pay LLM
costs on noise.** Naive "every anomaly triggers an LLM" pipelines fall over
the moment a flaky sensor or a benign deploy generates a handful of spikes.
CEP gives you the ability to demand a *pattern* — three anomalies in five
minutes on the same host — before the agent runs.

```
MetricSample  ─► AnomalyDetectFn ─► AnomalyEvent
                                       │
                                       ▼  CEP pattern (3-of-N within 5m)
                                       │
                                       ▼  IncidentEvent
                                              │
                                              ▼  IncidentAgentFn:
                                                   runbook (tool) → LLM plan → ticket (tool)
```

## Why GenericInferenceModel for anomaly detection?

`Classifier` returns a label + probability; `Scorer` returns a single
double. Anomaly detection wants both a continuous score *and* a discrete
"this is anomalous" verdict, plus implicit state (the rolling window).
That shape doesn't fit either typed surface cleanly.

`GenericInferenceModel.infer(Map<String, Object>, InferenceSetup) →
Map<String, Object>` is the escape hatch. The example wires a sliding-window
z-score as a `GenericInferenceModel`; the input map carries `{value: 920.0}`
and the output map carries `{zScore: 4.1, anomaly: true}`.

In production swap in a real autoencoder loaded through ONNX or DJL — the
same `GenericInferenceModel` shape works because the I/O is just maps.

## Why CEP rather than a window aggregate?

A 5-minute tumbling window with `count >= 3` would work for this specific
case but generalizes poorly. CEP shines when the *pattern* itself encodes
the policy:

- "Three anomalies followed by a recovery, then another anomaly" — easy in
  CEP, awkward in a window.
- "Anomaly on host A then host B in the same cluster" — easy in CEP.
- "Anomaly with no recovery within 10 minutes" — `within` + side outputs for
  timed-out matches.

The example uses the simplest version (three `.next()` legs) for
readability; the framework supports the full
`org.apache.flink.cep.pattern.Pattern` DSL.

## Listener-driven observability

The example doesn't wire the framework's listener SPI explicitly — the
metrics are inline. To plug into the same observability layer the other
examples use, register a `MetricsAgentEventListener` on the agent and fire
`listener.onInference(...)` from `AnomalyDetectFn`, and
`listener.onToolCallEnd(...)` from the agent operator. Recipes #9 and the
`MetricsAgentEventListener` reference impl in
`src/main/java/org/agentic/flink/listener/` cover the pattern.

## Cost shape

Sample stream → anomaly detector: O(1) per sample, in-process.
Anomaly stream → CEP pattern: O(1) per event, Flink-state-backed.
Incident stream → agent: one LLM call + two tool calls per *confirmed*
incident — orders of magnitude fewer than the underlying sample rate.

On a stream of 1k metric samples per minute with ~2% anomaly rate and a
five-minute pattern window, you'd expect maybe 1–2 confirmed incidents per
minute reaching the LLM. That's the design goal.

## Failure modes

- **Cold-start anomalies**: the z-score detector needs at least 5 samples in
  its window before it returns true. The example seeds with 18 normal
  samples for this reason.
- **CEP key cardinality**: the `keyBy(host)` shapes CEP state. Don't keyBy
  unbounded high-cardinality fields without a state TTL.
- **LLM plan grounded only in the runbook**: the agent's system prompt is
  strict about using the runbook excerpt; if the runbook says "open a ticket
  and tag #platform" that's the plan the LLM will produce.
