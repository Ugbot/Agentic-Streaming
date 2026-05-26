# Incident-detection agent example

Combines streaming anomaly detection (Generic inference model) with Flink CEP
pattern matching so the LLM is only invoked on confirmed incidents — not on
every metric sample.

```
MetricSample (host, metric, value, ts)
  │
  ▼  keyBy(host)
  │
  ▼  AnomalyDetectFn — GenericInferenceModel emits AnomalyEvent on z-score outliers
  │
  ▼  Flink CEP pattern: three anomalies within 5 minutes on the same host
  │
  ▼  IncidentEvent
  │
  ▼  keyBy(host)
  │
  ▼  IncidentAgentFn:
  │      runbook lookup (tool) → LLM remediation plan → ticket creation (tool)
  │
  ▼  output: "incident#N ticket=INC-1 plan=…"
```

## What's interesting

| Piece | API used |
|-------|----------|
| Anomaly detector | `GenericInferenceModel` — the SPI's escape hatch for non-typed I/O |
| Pattern matching | Flink CEP, `Pattern.begin("first").next("second").next("third").within(...)` |
| Runbook lookup | Plain `ToolExecutor` returning a runbook excerpt |
| Ticket creation | Plain `ToolExecutor` printing the ticket and returning its id |
| Cost shape | LLM fires once per confirmed incident, not per sample |

The anomaly detector here is a sliding-window z-score implemented in-process.
Swap it for an autoencoder loaded through DJL or ONNX without touching the
rest of the pipeline — that's the point of the SPI shape.

## Prerequisites

```bash
docker compose up -d ollama
docker compose exec ollama ollama pull qwen2.5:3b
```

## Run

```bash
mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.incident.IncidentAgentExample"
```

Or:

```bash
./examples-bin/run-incident.sh
```

## Expected output

```
📨 created INC-1 host=host-a metric=latency_ms
incident#1 ticket=INC-1 plan=1. Check the load balancer health page for host-a.
                              2. Inspect JVM GC pause times in the last 10 minutes.
                              3. Verify DB pool sizes aren't saturated.
                              4. Escalate to #platform if all three look healthy.
```

(Exact phrasing depends on the LLM; the structure should always be a short
runbook-grounded remediation plan plus a generated ticket id.)
