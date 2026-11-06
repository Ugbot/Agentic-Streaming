# Security model

This page states what the code does today, including where it does nothing. The project is
experimental. **Do not expose any of it to untrusted callers, and do not use it for real payments or
other non-idempotent actions.**

## The canonical path has no authentication

The canonical path is the `agentic/v1` runtimes: `WorkflowTurnFunction` on Flink, `LocalRuntime` in
the Java and Python cores, and the Pekko entity.

- None of them authenticates a caller. There is no principal in `AgentContext`: `user_id` is a string
  the event carries, and nothing checks it.
- The Pekko HTTP front door has no authentication. `POST /agent` runs a turn, and
  `GET /conversations/{id}` returns the whole event log, tool results included, to anyone who can
  reach the port. `HttpMain` binds `0.0.0.0` unless `AGENTIC_PEKKO_HTTP_HOST` says otherwise. Set it
  to `127.0.0.1` on any machine you do not fully control.
- The Flink JobManager REST API in `docker-compose-session.yml` has no authentication and accepts jar
  uploads and job submission. It is published on `127.0.0.1` only and must never be exposed beyond
  the host.

The bearer-token model that earlier versions of this page described belonged to the A2A gateway and
the tool services, which have moved out of this repository.

## The tool allowlist is only enforced in `LlmBrain`

A path's `tools` list is checked in one place: the LLM brain (`LlmBrain` in `ports/jagentic-core`,
the scripted LLM path of `ports/pyagentic/agentic/engine.py` in Python). It is not checked at the
tool boundary, so it is not a security control:

- The rule brain ignores `tools` entirely. A `tool_triggers` entry can run a tool the path does not
  list.
- `tools: []` means allow all, in both cores.
- Timer, CEP and saga calls bypass the check.
- Outside scripted mode, a refused tool is fed back to the model, which may try again.

The deprecated legacy DSL has its own check (`Agent.canUseTool`, called from `ToolExecutionEngine`).
It does not apply to the canonical path.

## Workflow documents are trusted input

Treat a workflow document or `pipeline.yaml` as code. `mcp[].command` starts a process when the
document is built, with environment expansion on its arguments. Load only documents you wrote or
reviewed.

## Input guardrails

Guardrails are regular expressions and classifiers you configure. The examples use one regex over
user text. Tool results and retrieved text are not checked. This is not a prompt-injection defence.

## Effects

A tool runs first and is recorded afterwards, and a function or HTTP tool receives no idempotency
key. After a Flink restore, or any redelivery of a turn that did not finish, tool calls run again. See
the status block in the [README](../README.md) and the [Flink runtime page](runtimes/flink.md).

## Egress policy

`org.agentic.flink.net.OutboundUrlPolicy` constrains outbound URLs in the Flink module's web toolkit
(`Fetcher`, and through it `WebFetchTool` and `CrawlerCore`). It does not apply to `kind: http` tools
or A2A peers in the cores: those URLs are operator supplied and are trusted.

The policy:

- accepts `http` and `https` only;
- rejects URLs with userinfo (`user:pass@host`);
- resolves the host and checks every returned address, not just the first;
- rejects loopback, link-local, RFC 1918 and other private ranges, the cloud metadata address
  `169.254.169.254`, unspecified and multicast addresses, IPv6 unique-local addresses, and
  IPv4 addresses embedded in IPv6 (mapped, NAT64 and 6to4 forms);
- optionally restricts hosts to an allowlist of exact names or `*.suffix` patterns, set with
  `WebToolkitOptions.withUrlPolicy(OutboundUrlPolicy.fromAllowlist(...))`.

Fetchers never let the HTTP client follow redirects on its own. Each `301`, `302`, `303`, `307` or
`308` location is validated against the same policy before it is followed, and the chain is capped
(five hops by default). `OutboundUrlPolicy.allowingPrivateAddresses()` turns the private-address
check off and is for development only.

## Compose stacks

The compose stacks publish every port on `127.0.0.1` and carry no committed credential.
`POSTGRES_PASSWORD` and `REDIS_PASSWORD` are required variables (`${VAR:?...}`), so compose refuses to
start until they are set in `.env`. Provider API keys (`OPENAI_API_KEY`, `GOOGLE_API_KEY`,
`ANTHROPIC_API_KEY`) are only ever read from the environment and have no committed values.

## Dependencies

The outbound A2A client is built on the `a2a-java` SDK at an alpha version (`1.0.0.Alpha3`). Tika
(2.9.2) and DJL (0.30.0) in the root module carry known critical CVEs (CVE-2025-66516, CVE-2025-54988,
CVE-2025-0851) and have not been upgraded yet.
