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

The outbound A2A client is built on the `a2a-java` SDK at an alpha version (`1.0.0.Alpha3`). No
advisory names it, but upstream has moved to `org.a2aproject.sdk`, and `SdkA2AClient` has no test
against a real peer to prove the move.

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
| Netty 4.1.100, Jackson 2.15.3, Guava 32.1.3, lz4 1.8.0 (`com.alibaba.fluss:fluss-client` 0.7.0) | Fluss client, relocated. **Not in the default uber jar**: `provided`, and bundled only by `-P fluss` | As for the rows above, plus CVE-2025-66566 and CVE-2025-12183 in lz4 1.8.0 | Not in a default build. With `-P fluss`, or the client added by hand, only when a Fluss backend is selected, and then the client talks only to the operator's Fluss cluster, so a hostile response needs a hostile cluster. `org.apache.fluss:fluss-client` 1.0.0 does not help: it shades Netty 4.1.104, Jackson 2.15.3 and Guava 32.1.3. |
| Netty 4.1.127 (`io.milvus:milvus-sdk-java` 2.6.27, under `io/milvus/shaded`) | Milvus SDK, relocated, and in the default uber jar | At least 31 in `netty-handler`, `netty-codec`, `netty-codec-http` and `netty-codec-http2` | Only when the Milvus backend is selected, and then only as a client of the operator's Milvus server, so a hostile response needs a hostile server. The SDK builds on gRPC 1.77.0, and that release of `grpc-netty-shaded` carries 4.1.127; the relocated copy has no version file of its own. No upgrade helps: 3.0.12 is on gRPC 1.77.0 too. |

The Fluss client also brought an unrelocated RocksDB into the uber jar, which went with it. Tests
still see the client, because `provided` is on the test classpath.

The A2A BOM holds gRPC at 1.77.0, so the root pom imports `grpc-bom` 1.84.2 ahead of it, which is what
the Qdrant client declares. Without that, `grpc-netty-shaded` puts a relocated Netty 4.1.127 in the
uber jar under `io/grpc/netty/shaded`. It carries 4.2.16 now, which has no OSV advisory.

Quarkus (CVE-2026-50559, CVE-2026-39852) left with the A2A gateway and the tool services, so no
manifest here resolves it.

### Upgrades proven only at compile level

These versions build and pass their unit tests, and nothing here has run them against the real
service:

- Milvus SDK 2.6.27. `MilvusVectorStore` compiles and `VectorStoreDiscoveryTest` finds it through
  `ServiceLoader`. No test talks to a Milvus server, at this version or any other.
- The Fluss client. `FlussChannelIT` and `FlussConversationStoreIT` start their cluster with host
  networking, and failed to start it on Docker Desktop for macOS, so they need a Linux host.
  `FlussClientAbsentTest` covers the jar without the client.
- `pekko-persistence-cassandra` 1.1.0 on Pekko 1.7.1, without a Cassandra.
- `kafka-clients` 4.1.2 (Flink) and 3.9.2 (Pekko). No `*IT` class starts a broker.
- The A2A gRPC transport, built against gRPC 1.77.0 and resolved at 1.84.2. `SdkA2AClient` has no
  test against a real peer.
- DJL 0.40.0 with a live model. The `djl` tag stays out of the default and integration runs.
