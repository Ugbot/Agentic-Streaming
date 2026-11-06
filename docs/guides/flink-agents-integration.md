# Apache Flink Agents and this project

[Apache Flink Agents](https://github.com/apache/flink-agents) is an Apache project for running LLM
agents on Flink. This project is a portable specification and conformance suite for agents, with
runtimes on Flink, plain Python and Pekko. They overlap on Flink, and they do not depend on each
other. An earlier plugin that tried to bridge them was dead code and has been removed.

**If you are on Flink and want LLM agents, Flink Agents is the better choice today.**

This comparison is against Flink Agents 0.4.0 (tag `release-0.4.0`, October 2026), read from its
source on 2026-11-06. Nothing of it was built or run for this page. Its 0.x line is a preview with
experimental APIs, by its own description.

## Where they differ

| | Apache Flink Agents 0.4.0 | This project |
|---|---|---|
| What it is | An agent framework on Flink | A versioned spec and conformance fixtures, with runtimes on Flink, plain Python and Pekko |
| Status | Preview; four minor lines shipped (0.1 to 0.4) | Experimental; nothing shipped |
| Packages | Maven Central and PyPI | None |
| Flink | 1.20 to 2.3, Java 11 target | 2.2.1, Java 21 |
| Languages | Java and Python, at parity | Java and Python cores, separate implementations |
| Execution on Flink | Non-blocking, including async actions and parallel tool batches | Blocks the task thread on every model and tool call |
| After a crash | Recorded call outcomes are reused from an action state store (Kafka or Fluss) | Tool and model calls in an unrecorded turn run again |
| Model providers | Anthropic, Bedrock, Gemini, Ollama, OpenAI and Watsonx in Java; more in Python | Only a scripted stub on the Flink spec path |
| Tool calling and structured output | Native where the provider supports it | Tool calls from a bounded loop; structured output by prompt instruction and parsing |
| MCP | Client through the official SDK: HTTP, tools, prompts, auth, retry | Stdio on the canonical path, plain POST on the legacy path; 2024-11-05 revision; also a small server |
| Observability | Metrics and a trace log | Listeners and a `Tracer` SPI; no OpenTelemetry exporter |
| Table API, skills, sub-agents, multimodal input | Yes | Table API no; a smaller `Skill`; delegation as a tool over A2A; no multimodal |
| Running without Flink | No | Yes: plain Python and Pekko, same workflow document |
| Behavioural spec and fixtures across runtimes | No | Yes: `spec/v1`, 24 fixtures |
| Guardrails and verifiers | No | Yes |
| Saga, CEP, suspend and resume, declarative timers | No | Yes, with limits: a saga is not crash-safe |
| Outbound A2A | No | Yes |

Even Flink Agents' recovery is limited, and it says so: it reuses recorded outcomes, and it cannot
make an effect on an outside system transactional.

## When to choose which

Choose Flink Agents if you run on Flink and want agents in production soonest.

Consider this project if:

- the agent must not depend on Flink, and you want the same workflow on plain Python, the JVM and
  Pekko, with behaviour pinned by fixtures;
- you want routing, verification, guardrails, timers and CEP declared in a document, and can accept
  that on Flink this holds today only for model-free or stubbed agents;
- you are interested in the spec itself.

What this project cannot give you yet is in the [README](../../README.md).
