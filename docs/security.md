# Security model

This page states what the code does today, including where it does nothing. The project is
experimental and has had no outside security review. **Do not expose it to untrusted callers, and do
not use it for real payments.**

## Who is calling

An event can carry a `Principal` (`subject`, `tenant`, `roles`, `issuer`). Beside one, `user_id` means
nothing: the turn runs as the principal, and it is recorded on `turn_received`, `tool_intent` and
`tool_refused`. Whatever delivers the event vouches for it, so **anyone who can write to the input
topic can claim any principal**. Keep that topic to trusted producers.

Only an authenticated principal named by `policies.approvers` can resume a suspended turn or settle
an unknown outcome. Nobody can if the block is absent. A refused signal is `signal_refused` on the log
and changes nothing. A resumed turn keeps the principal that started it, and the approver is recorded
apart. The fixtures `principal-on-the-log` to `wrong-tenant-turn` prove this on all five runtimes.

- The Flink JobManager REST API in `docker-compose-session.yml` has no authentication and accepts jar
  uploads and job submission. It is published on `127.0.0.1` only and must never be exposed beyond
  the host.

## The gateway

`agentic-gateway` is the one HTTP front door. The Pekko routes are gone. It serves `POST /agent` (run a
turn), `GET /conversations/{id}` (read the log) and `POST /conversations/{id}/turns/{turn}/resume`
(answer a suspended turn), and it runs in front of Flink over Kafka (`GatewayMain`), Pekko (`HttpMain`)
or a `LocalRuntime`. `GET /healthz` and the Agent Card are the only routes open without a token.
Python has no HTTP front door.

**It fails closed.** With no authentication configured it does not start. It binds `127.0.0.1` unless
`AGENTIC_GATEWAY_HOST` says otherwise, and it speaks plain HTTP, so it refuses any other address
unless you also set `AGENTIC_GATEWAY_BEHIND_TLS_PROXY=true`.

**TLS.** Terminate it in front of the gateway, with a reverse proxy or a mesh sidecar on the same host
or network segment, and bind the gateway to an address only that proxy can reach. The gateway does not
read `X-Forwarded-*`, so nothing a proxy passes on changes who it thinks is calling.

**Authentication.** A bearer token on every call:

| Setting | Meaning |
| --- | --- |
| `AGENTIC_OIDC_ISSUER` | the exact `iss`. Setting it selects OIDC |
| `AGENTIC_OIDC_AUDIENCE` | the `aud` a token must name. Required |
| `AGENTIC_OIDC_JWKS_URL` | default `<issuer>/protocol/openid-connect/certs` |
| `AGENTIC_OIDC_TENANT_CLAIM` | default `tenant` |
| `AGENTIC_OIDC_ROLES_CLAIM` | default `realm_access.roles`, dotted for a nested claim |
| `AGENTIC_GATEWAY_STATIC_TOKENS` | `token=subject\|tenant\|role,role;...`, local development only. Needs `AGENTIC_GATEWAY_ALLOW_STATIC_TOKENS=true`, works on loopback only, and refuses a token under 16 characters |
| `AGENTIC_GATEWAY_READER_ROLES` | roles that may read any conversation of their tenant |

A token is checked against the issuer's JWKS, then for issuer, audience, expiry and not-before. Only
RS256, PS256 and ES256 are accepted, so `alg: none` and an HMAC made with the public key both fail.
`OidcAuthenticatorTest` signs a token for each of these and more. A token with no tenant gets a 403.

**A request body cannot say who is calling.** `user_id`, `principal` and `tenant` in a body are not
read. The principal comes from the token.

**Tenancy.** The runtime's conversation key is `<tenant>/<conversation_id>`, each part encoded, so
another tenant cannot name a conversation at all. Within a tenant, only the subject that started a
conversation can add a turn or read it, and a reader role can read it. Everyone else gets the 404 that
a conversation with no turns gets, every read.

**Approvals.** A resume comes with the approver's token, and the runtime checks it against
`policies.approvers`. A refusal is a 403 and `signal_refused` on the log.

**In front of Flink** the gateway writes turns to the input topic with the verified principal and
reads answers from the results topic. A conversation is read from that topic, so it shows no further
back than the topic's retention, and an answer the job does not give in time is a 504: redeliver the
same `turn_id`. Anything else that can write the input topic can claim any principal.
`GatewayOverFlinkTest` proves the path over a real Flink job, through a stop-with-savepoint, using the
in-process session in place of topics. `KafkaBackend` itself is proved against Kafka's mock client.

## The tool boundary

Every tool call passes one deny-by-default check in `AgentContext.invoke` (the Java core, so Flink and
Pekko too) and `Engine._invoke` (Python), whether a brain, a saga, a timer or a CEP pattern asked for
it. `spec/v1/primitives.md`, section 11, is the rule. The conformance fixtures `rule-tool-outside-path`
to `refusal-survives-a-crash` prove it on the reference, Java core, Flink, Pekko and Python runtimes.

- A path gets what its `tools` lists and nothing else. `tools: []` means none, and so does leaving it
  out. `tool_triggers`, saga steps and timers name a tool; they do not grant it.
- Timers and CEP patterns run outside any path, so what they may call is `policies.unattended_tools`.
- A refused call is recorded as `tool_refused` and ends the turn with class `policy`. It is not
  offered back to the model.
- Java code that builds a `RoutedGraph` itself has to give it a `ToolPolicy`. Without one every call
  is refused. `ToolPolicy.ALLOW_ALL` exists for such code and a document cannot reach it.

## Open Policy Agent

Set `AGENTIC_OPA_URL` and every tool call the document allows is also put to OPA, in Java and Python
alike. `ToolPolicy` is the SPI, so another engine is one class. OPA runs after the document and can
only narrow it: a call the document refuses never leaves the process.

The input is `{tool, origin, path, effect, principal, args}`, so tool arguments reach OPA. The answer
at `AGENTIC_OPA_POLICY` (default `agentic/tools`) is a boolean, or an object with `allow` and
optionally `reason`. Anything but a clear yes refuses the call: an OPA that is down, slow past
`AGENTIC_OPA_TIMEOUT_MS` (default 2000), undefined or confused. So an OPA outage stops every tool.

A call already begun is not asked about again, so a re-run of a turn takes the path the first run took
even if the policy has changed since.

`examples/compose/opa-lab.yml` is a lab with a sample policy, and `tools/opa-lab.sh` runs the Python
core against it. `tools/keycloak-lab.sh` does the same for the gateway against a real Keycloak.

## Workflow documents are trusted input

Treat a workflow document or `pipeline.yaml` as code, and load only documents you wrote or reviewed.
It chooses the tools, the URLs and the policy, and validation does not make a hostile one safe.

`mcp[].command` starts a process when the document is built. It starts only if the operator listed it
in `AGENTIC_MCP_ALLOWED_COMMANDS` (system property `agentic.mcp.allowed_commands`), and with no list
nothing starts. Entries are separated by `;`, and each is a command with the arguments it must start
with, matched word for word after environment expansion: `python3 examples/pipelines/mcp_util_server.py`
allows that script and not `python3 -c ...`. A bare `python3` allows whatever Python will run. On Flink
the list is read on the TaskManager.

## Prompt injection

**The defence is the tool policy, not a filter.** Guardrails are regular expressions and classifiers
you configure. They see user text and the final reply, and they will not stop an injection: nothing
here can stop a model being talked into asking for something. What holds is that asking is not
enough.

- A tool the path does not list is refused, whatever the model read. `injected-text-cannot-reach-a-denied-tool`
  proves this on all five runtimes with a model that obeys the tool result, and `InjectionTest` and
  `test_an_injected_call_to_a_denied_tool_is_refused...` prove it through the real loop.
- `policies.tool_approval: {effects: [non_idempotent]}` puts a person in front of those tools. The
  turn suspends before the call and an approver answers `approved` true or false, which is
  `injected-text-reaches-the-human-gate`. A timer or a CEP pattern has nobody to ask, so such a tool is
  refused there. Without this block an allowed tool runs when a model asks for it, injected or not.
- Tool results go into the prompt between `<untrusted>` tags, with any such tag inside defused, and
  the system prompt says what that means. This helps a model. It is not a boundary.

Left out: the LLM brain puts no retrieved text in a prompt, so nothing marks it there. A custom brain
that does should use `Untrusted.mark`, or `agentic.untrusted.mark` in Python. A reply can still repeat
what a tool said, and a path's own allowed tools can still be misused within what they allow, so give
a path the fewest tools that will do.

## Limits

`policies.limits` sets per-principal `turns_per_minute`, `tokens_per_day`, `cost_per_day` with
`cost_per_1k_tokens`, and `tool_calls_per_day` by tool. A limit that is hit is `limit_exceeded` on the
log, the turn ends with class `policy`, and the gateway answers 429. `spec/v1/primitives.md`, section
11, has the rules, and the fixtures `turn-rate-limit` to `redelivery-is-not-a-new-turn` prove them on
all five runtimes.

**The kill switch** stops every turn, tool call and model call. `kill_switch: true` in the document
does it for one workflow. `AGENTIC_KILL_SWITCH=true`, or creating the file `AGENTIC_KILL_SWITCH_FILE`
names, does it for a runtime, and the file takes effect on the next turn or call with no restart.
On Flink the file must be visible to every TaskManager.

Three things to know:

- **The counters are in memory.** They are per process and a restart clears them. On Flink that means
  per subtask, so with parallelism 4 one principal can get up to four times a limit, across
  conversations on different subtasks. A limit here slows a principal down. It is not a hard ceiling.
- **Tokens are an estimate,** four characters each. The clients read no provider usage, so a budget is
  not what the provider will bill.
- A turn stopped by a limit is spent, so retry with a new `turn_id`.

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
  and a person answers with a signal. That answer is privileged, because `not_applied` makes the
  call again, so only an approver can give it.
- A timeout, a lost connection or an HTTP 5xx from a `non_idempotent` tool is unknown in the same way.
- Without a key vault the journal holds results in plaintext, as the log does. Protect and expire it
  as you would the log, and keep entries longer than the longest rollback or redelivery you allow.
  An entry dropped sooner re-opens that turn to repeat its calls.
- A worker that lost its lease can still be running a call while its replacement starts. There is no
  fencing token at the tool, so a tool must not rely on the journal alone to exclude that. A Kafka
  journal has no compare-and-set at all, so its two writers can both see no entry and both call;
  Postgres stops the second at the intent, by its primary key.
- A journal that cannot be reached stops the call: the intent is written first, and a failed write
  ends the delivery, on Flink by failing the task. `on_unavailable: degrade` falls back to memory only
  when no tool is `non_idempotent`. Journal reads and writes are bounded, at 15 seconds on a Postgres
  socket and 30 on Kafka, so a stalled store fails the task rather than hanging it.
- A journal entry cannot answer a different call: its intent is a SHA-256 of the agent id, the tool
  and the arguments, and a mismatch ends the turn `fatal` with nothing called. Two agents with the
  same `agent.id` on one store are not told apart.
- Model keys come from the environment (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`) and
  travel in headers. A document that carries one is refused. A provider's error body reaches logs and
  the turn's error cut to 200 characters with the key removed, but what else it says is the provider's.

See [Guarantees](../README.md#guarantees) and the [Flink runtime page](runtimes/flink.md).

## Egress policy

Every `kind: http` and `kind: agent` tool, every A2A peer over HTTP and every model call checks its
URL before sending anything: `org.jagentic.core.net.Egress` over `OutboundUrlPolicy` in Java,
`agentic.egress` in Python. The check runs on every call. A refused call is a definite failure,
`<tool> failed: blocked by egress policy`, so a `non_idempotent` tool does not wait for a person. A
refused model call fails the turn. The fixtures `egress-metadata-address`, `egress-not-http` and
`egress-private-address` prove it on all five runtimes.

The policy:

- accepts `http` and `https` only;
- rejects URLs with userinfo (`user:pass@host`);
- resolves the host and checks every returned address, not just the first;
- rejects loopback, link-local, RFC 1918 and other private ranges, the cloud metadata address
  `169.254.169.254`, unspecified and multicast addresses, IPv6 unique-local addresses, and
  IPv4 addresses embedded in IPv6 (mapped, NAT64 and 6to4 forms).

**This denies local services by default.** The settings are the operator's, and a document cannot
change them:

| Setting | Meaning |
| --- | --- |
| `AGENTIC_EGRESS_ALLOW_HOSTS` | exact names or `*.suffix`, comma separated. Empty means any public host |
| `AGENTIC_EGRESS_ALLOW_PRIVATE=true` | turns the private-address check off, for an Ollama or a peer on this network. Pair it with an allowlist |

On Flink these go in the TaskManager's environment. In Java the system properties
`agentic.egress.allow_hosts` and `agentic.egress.allow_private` do the same.

What it does not cover:

- A check resolves the host and the client resolves it again to connect, so a name that changes
  between the two gets through.
- Redirects are not followed at all: a 3xx is a definite failure.
- `LangChain4jChatClient`, embedders, vector stores and the journal connect to what the operator
  configured. They do not go through this check.

## Secrets and data

**Per-tool secrets are references.** `tools[].auth: {bearer: env:NAME}` or `file:/path`, and the same on
`a2a[]`. It is resolved at the moment of the call, so it reaches the wire and nothing else, and a
document that writes the secret itself is rejected when loaded. A `kind: http` tool, a Java A2A peer and
the older Python engine's peer use it. Python's canonical engine has no HTTP peers.

**Redaction** runs before the log and the journal are written, in all five runtimes. Credentials are
always covered: fields such as `password`, `token`, `api_key` and `authorization`, and text shaped like
a bearer token, an OpenAI or AWS key, or a JWT. `policies.redact` adds your own `fields` and `patterns`
for personal data. It finds what the patterns describe and nothing else, so it narrows what is recorded
and does not promise it is clean. The reply the caller receives is not redacted, and neither is a
results topic's `reply` field.

**Deletion on an immutable log is crypto-shredding.** Set `AGENTIC_KEY_VAULT_DIR` and every conversation
gets its own AES-256-GCM data key: `text`, `reply`, `args`, `result`, `messages`, `signal`, `error` and
`reason` are sealed before they reach the log or the journal. To honour a deletion request, run
`org.jagentic.core.privacy.Erase tenant/subject`. It destroys the keys of every conversation that owner
started, and those fields read as `[erased]` from then on, with the log still folding. Keys are
per conversation, so a turn after an erasure gets a new key that cannot open the old data.
`SealingTest` proves this on the Java core and `WorkflowTurnSealedTest` on Flink, through a restore.

What this leaves out, plainly:

- **Pekko's log and Python are not sealed.** On Pekko only the journal is. Crypto-shredding is the Java
  core and Flink.
- **The Flink results and failures topics are not sealed.** A result carries the turn's open events, so a
  deletion request is not honoured there. Keep those topics on a short retention.
- **What stays readable:** the owner's identifier, tenant, tool ids, paths, event types and times. That
  is how ownership works, and it is personal data if a subject is. Use an opaque subject.
- **A journal intent is a digest of the arguments,** so low-entropy arguments can be guessed from one
  after an erasure.
- `FileKeyVault` deletes files, which does not scrub a disk, a snapshot or a backup, and every process
  needs the same directory. For anything real, implement `KeyVault` over a KMS.
- Text a person pasted into a turn about someone else is sealed under the conversation's owner, so
  erasing that other person does not reach it.
- A Flink checkpoint taken earlier still holds the sealed bytes, which is the point: without the key
  they do not open.

**Retention.** The rules are per store, and there is no single setting:

| Store | What expires it |
| --- | --- |
| Flink keyed state | `runtime.flink.state_ttl_ms`: a conversation idle that long is cleared whole, unless a turn is suspended |
| Kafka topics, Kafka journal | the topic's `retention.ms`, which the operator sets |
| Postgres journal, Pekko journal | nothing here: the operator must prune them |
| Python file log | nothing here |

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

