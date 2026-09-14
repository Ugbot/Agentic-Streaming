# Security model

This page describes how the network-facing surfaces of this repository authenticate callers,
how outbound HTTP egress is constrained, and which defaults exist only for local development.
Everything here refers to the Flink main module, the Quarkus A2A gateway (`a2a-gateway/`),
the Quarkus tool services (`tool-services/`) and the FastAPI gateway
(`ports/experimental/gateway-fastapi/`). The compose stacks in the repository root follow the same rules.

## Token model

All HTTP, SSE, gRPC and MCP entry points use the same scheme: a shared bearer token presented
in the `Authorization: Bearer <token>` header (gRPC uses the `authorization` metadata key).
Tokens are compared in constant time. A request with no header, a non Bearer scheme, an empty
token or an unknown token is rejected with HTTP 401 and `WWW-Authenticate: Bearer`
(gRPC: `UNAUTHENTICATED`). Health probes and the public A2A Agent Card do not require a token.

Every service fails closed: when no token is configured, every protected request is rejected.
The only way to run without a token is the explicit development override listed below.

| Surface | Token setting | Development override | Notes |
|---------|---------------|----------------------|-------|
| A2A gateway (JSON-RPC, SSE, `/rag/ingest`, `/rag/query`) | `a2a.auth.tokens` (`AGENTIC_FLINK_A2A_AUTH_TOKENS`) as `subject=token,subject2=token2` or a bare token; fallback `AGENTIC_A2A_TOKEN` | `a2a.auth.dev.mode=true` (`AGENTIC_FLINK_A2A_AUTH_DEV_MODE`) | The subject is recorded as the task owner. A bare token maps to the subject `a2a-client`; the dev override maps to `dev-anonymous`. |
| Tool services (REST, MCP, gRPC) | `TOOL_SERVICES_TOKEN` (`tools.auth.token`) | `TOOL_SERVICES_AUTH_DEV_MODE=true`; on by default only under the Quarkus `dev` profile | Swagger UI is served only under the `dev` profile. |
| FastAPI gateway (`/agent`, `/conversations/*`) | `AGENTIC_GATEWAY_TOKENS` as `alice=tok-a,bob=tok-b`, or `AGENTIC_GATEWAY_TOKEN` for one token mapped to `gateway-client` | `AGENTIC_GATEWAY_AUTH_DEV_MODE=true` | The subject is the user id passed to the backend. |

### Identity and ownership

The caller identity is derived only from a validated token. Raw `Authorization` headers are
never copied into task claims or forwarded to the Flink job; the claims carry the subject and
the scheme (`Bearer` or `dev`).

A2A tasks created by `message/send` and `message/stream` record the caller subject in the task
metadata under the key `owner`. `tasks/get`, `tasks/cancel` and the four
`tasks/pushNotificationConfig/*` methods compare the caller against that owner and answer with
JSON-RPC error `-32003` (Forbidden) on a mismatch, so a task id alone is not enough to read,
cancel or redirect somebody else's task.

The FastAPI gateway scopes conversations per principal. The backend key is
`<subject>/<conversation_id>`, so two principals using the same conversation id get two
independent transcripts, and a request body `user_id` that differs from the authenticated
subject is rejected with HTTP 403. `conversation_id` must not contain `/`.

Tool invocation inside a Flink job checks the active agent's tool allowlist
(`Agent.canUseTool`) before the registry lookup. A call to a tool the agent is not allowed to
use fails with `Tool not permitted for this agent` and the tool is never executed.

## Egress policy

Any outbound URL that comes from untrusted input (model output, request bodies, stored push
notification webhooks) passes through `OutboundUrlPolicy` before a connection is opened. The
policy lives in `org.agentic.flink.net.OutboundUrlPolicy` for the main module and the A2A
gateway, and in `org.jagentic.tools.web.OutboundUrlPolicy` for the tool services.

The policy:

- accepts `http` and `https` only;
- rejects URLs with userinfo (`user:pass@host`);
- resolves the host and checks every returned address, not just the first;
- rejects loopback, link-local, RFC 1918 and other private ranges, the cloud metadata address
  `169.254.169.254`, unspecified and multicast addresses, IPv6 unique-local addresses, and
  IPv4 addresses embedded in IPv6 (mapped, NAT64 and 6to4 forms);
- optionally restricts hosts to an allowlist of exact names or `*.suffix` patterns.

Fetchers never let the HTTP client follow redirects on its own. Each `301`, `302`, `303`,
`307` or `308` location is validated against the same policy before it is followed, and the
chain is capped (five hops by default). Connect and request timeouts are always set.

| Consumer | Allowlist | Private address override (development only) |
|----------|-----------|----------------------------------------------|
| Main module `Fetcher`, `WebFetchTool`, `CrawlerCore` | `WebToolkitOptions.withUrlPolicy(OutboundUrlPolicy.fromAllowlist(...))` | `OutboundUrlPolicy.allowingPrivateAddresses()` in code |
| Tool services web pack | `TOOL_WEB_ALLOWED_HOSTS` (comma separated) | `TOOL_WEB_ALLOW_PRIVATE=true` |
| A2A push notification webhooks | `a2a.push.allowed.hosts` (`AGENTIC_FLINK_A2A_PUSH_ALLOWED_HOSTS`) | none; a stored webhook is validated again immediately before each delivery and skipped if it no longer passes |

Remote agent URLs that come from configuration (`RemoteAgentSpec`) are operator supplied and
are trusted; they are not subject to this policy.

## Development-only defaults

The following settings are acceptable on a developer machine and must not be used in any shared
or exposed deployment:

- `a2a.auth.dev.mode=true`, `TOOL_SERVICES_AUTH_DEV_MODE=true`,
  `AGENTIC_GATEWAY_AUTH_DEV_MODE=true`: admit unauthenticated callers when no token is set.
- `TOOL_WEB_ALLOW_PRIVATE=true` or `OutboundUrlPolicy.allowingPrivateAddresses()`: let
  fetchers reach loopback and private networks.
- The Quarkus `dev` profile for the tool services: enables Swagger UI and the auth override.
- The Flink JobManager REST API in `docker-compose-session.yml` and `docker-compose-cluster.yml`:
  it has no authentication and accepts jar uploads and job submission. It is published on
  `127.0.0.1` only and must never be exposed beyond the host.

The compose stacks publish every port on `127.0.0.1`, and none of them carry a committed
credential. `POSTGRES_PASSWORD`, `REDIS_PASSWORD`, `JUPYTER_TOKEN`, `AGENTIC_A2A_TOKEN` and the
banking demo tokens are required variables (`${VAR:?...}`), so `podman-compose` refuses to start
until they are set in `.env`. Provider API keys (`OPENAI_API_KEY`, `GOOGLE_API_KEY`,
`ANTHROPIC_API_KEY`) are only ever read from the environment and have no committed values.

## Dependency notes

The A2A gateway and the outbound A2A client are built on the official `a2a-java` SDK at an
alpha version (`1.0.0.Alpha3`). That SDK parses untrusted network input on a security boundary.
It is kept deliberately because it is the reference implementation of the protocol, but its
version should be tracked closely and upgraded as stable releases appear. The gateway's own
authentication, ownership and webhook checks described above run before any request reaches
the SDK types.
