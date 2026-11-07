# Security model

This page states what the code does today, including where it does nothing. The project is
experimental. **Do not expose any of it to untrusted callers, and do not use it for real payments:
the effect journal below stops repeated calls, but nothing here authenticates who asked for one.**

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

## Workflow documents are trusted input

Treat a workflow document or `pipeline.yaml` as code. `mcp[].command` starts a process when the
document is built, with environment expansion on its arguments. Load only documents you wrote or
reviewed.

## Input guardrails

Guardrails are regular expressions and classifiers you configure. The examples use one regex over
user text. Tool results and retrieved text are not checked. This is not a prompt-injection defence.

## Effects

Every tool and model call goes through the effect journal: intent is written first, the outcome
after, and a re-run reuses what is recorded. Every tool receives a deterministic `Idempotency-Key`,
as a header on HTTP and agent tools, in `_meta` on MCP tools, and with the call's identity in process.
The key is a SHA-256 of the conversation id, turn id and call index, so it leaks neither.

- A tool declares `effect: read`, `idempotent` or `non_idempotent`. **A tool that declares nothing is
  `non_idempotent`**, so it is never called twice, gets one attempt, and a call with no recorded
  result waits for a person. Only a tool that is safe to repeat should say otherwise. The same goes
  for a peer, whose client retries are switched off, and an MCP server.
- **A crash during a call to a `non_idempotent` tool leaves the outcome unknown.** The journal cannot
  tell whether the effect landed. The call is not repeated: the turn suspends with `unknown_outcome`
  and a person answers with a signal. Treat that person's answer as privileged, because
  `not_applied` makes the call again. Nothing yet checks who sent it; the sender is recorded as
  `signal_sender` and is not authenticated.
- A timeout, a lost connection or an HTTP 5xx from a `non_idempotent` tool is unknown in the same way.
- The journal holds tool arguments and results in plaintext, as the log does. Protect and expire it
  as you would the log, and keep entries longer than the longest rollback or redelivery you allow.
  An entry dropped sooner re-opens that turn to repeat its calls.
- A worker that lost its lease can still be running a call while its replacement starts. There is no
  fencing token at the tool, so a tool must not rely on the journal alone to exclude that.

See [Guarantees](../README.md#guarantees) and the [Flink runtime page](runtimes/flink.md).

## Egress policy

`org.jagentic.core.net.OutboundUrlPolicy` is in the core and nothing calls it yet. The Flink web
toolkit that used it is gone. It is there for the first tool that takes a URL from a model. `kind:
http` tools and A2A peers do not use it: those URLs are operator supplied and are trusted.

The policy:

- accepts `http` and `https` only;
- rejects URLs with userinfo (`user:pass@host`);
- resolves the host and checks every returned address, not just the first;
- rejects loopback, link-local, RFC 1918 and other private ranges, the cloud metadata address
  `169.254.169.254`, unspecified and multicast addresses, IPv6 unique-local addresses, and
  IPv4 addresses embedded in IPv6 (mapped, NAT64 and 6to4 forms);
- optionally restricts hosts to an allowlist of exact names or `*.suffix` patterns, set with
  `OutboundUrlPolicy.fromAllowlist(...)`.

A caller must not let its HTTP client follow redirects on its own: validate each location against
the same policy, and stop after five hops by default. `OutboundUrlPolicy.allowingPrivateAddresses()`
turns the private-address check off and is for development only.

## Compose stacks

The compose stacks publish every port on `127.0.0.1` and carry no committed credential.
`POSTGRES_PASSWORD` and `REDIS_PASSWORD` are required variables (`${VAR:?...}`), so compose refuses to
start until they are set in `.env`. `OPENAI_API_KEY` is only ever read from the environment and has no committed value.

## Dependencies

The resolved test-scope trees of `ports/jagentic-core`, the Flink module and `agentic-pekko`, and both
`uv.lock` files, have no open advisory in [OSV](https://osv.dev) at any severity. Transitive versions
are pinned in `dependencyManagement`, so read `./mvnw dependency:tree` rather than the declared
versions. The two `uv.lock` files cover Python 3.10 and later, because the dev tools have no patched
release for 3.9. The packages still declare `>=3.9`: at runtime they need only PyYAML and jsonschema,
neither of which has an advisory, but `pip install ".[test]"` on 3.9 gets a pytest with
CVE-2025-71176.

OSV matches coordinates, so it cannot see into a shaded jar. The table below is what that leaves.

### Known dependency advisories

Flink stays on 2.2.1 because the Kafka connector does not support 2.3. Everything in the first five
rows is in Flink's own jars on the cluster. The build marks them `provided`, and the floors in
`reactor/pom.xml` reach only the test classpath, so they cannot be overridden from here. The one
exception is `flink-shaded-guava`, which resolves at compile scope, so the uber jar carries its own
copy of the same version.

| Package, as shipped | Arrives through | Advisories | Reachable? |
| --- | --- | --- | --- |
| Netty 4.1.100 (`flink-shaded-netty` 4.1.100.Final-20.0) | Flink, relocated | Critical CVE-2026-75595, and 36 others in `netty-handler`, `netty-codec`, `netty-codec-http`, `netty-codec-http2` and `netty-common` (15 high, 20 moderate, 1 low) | Yes, by anything that can reach the JobManager REST port or the TaskManager data ports. Not by agent input: events, tool results and model output never touch these servers. Keep those ports on a private network, which the compose stacks do. |
| Jackson 2.18.2 (`flink-shaded-jackson` 2.18.2-20.0) | Flink, relocated | 17 in `jackson-core` and `jackson-databind`, including CVE-2026-89425, CVE-2026-89407, CVE-2026-91777 and CVE-2026-91776, mostly denial of service | Only through Flink's REST API and internal plan handling, so the same boundary as Netty. The framework parses JSON with its own Jackson 2.22.4. |
| Guava 33.4.0 (`flink-shaded-guava` 33.4.0-jre-20.0) | Flink, relocated | CVE-2026-102554, high: memory exhaustion on Java deserialisation of some collections | Only if an attacker can feed Flink's Java deserialisation, at which point this is the lesser problem. |
| ZooKeeper 3.7.2 (`flink-shaded-zookeeper-3` 3.7.2-20.0) | Flink, relocated | CVE-2024-23944, moderate: information disclosure in persistent watchers | Only with ZooKeeper HA, which no stack here configures. The advisory is on the server side of watch handling. |
| snappy-java 1.1.10.7 and lz4-java 1.10.3 | Flink, unshaded | snappy: CVE-2026-93451, CVE-2026-108106 (high), CVE-2026-116590. lz4: CVE-2026-106451 (high), CVE-2026-106449, -106450, -106452, -106453, CVE-2026-59949 | Flink feeds them its own state and network buffers, not attacker-chosen bytes, so the decompression bugs need the checkpoint store or the data plane to be compromised first. CVE-2026-106451 replaces the native library in a shared temp directory and needs a local user on the TaskManager host. Job code that bundles the uber jar gets the patched versions (1.1.10.12, 1.13.0) only where the job's classloader wins. |

Quarkus (CVE-2026-50559, CVE-2026-39852) left with the A2A gateway and the tool services, so no
manifest here resolves it.

### Upgrades proven only at compile level

These versions build and pass their unit tests, and nothing here has run them against the real
service:

- `pekko-persistence-cassandra` 1.1.0 on Pekko 1.7.1, without a Cassandra.
- `kafka-clients` 4.1.2 (Flink) and 3.9.2 (Pekko). No `*IT` class starts a broker.

