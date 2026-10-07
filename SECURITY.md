# Security policy

## Supported versions

Nothing is published yet: there are no releases, no tags and no artifacts on Maven Central,
PyPI or Clojars. The only supported version is the `main` branch of
https://github.com/Ugbot/Agentic-Streaming. Security fixes land on `main`; there are no
maintenance branches to backport to.

| Version | Supported |
|---------|-----------|
| `main`  | yes       |
| anything else (forks, local builds of older commits) | no |

## Reporting a vulnerability

Do not open a public issue for a security problem. Use GitHub's private vulnerability
reporting for this repository:

https://github.com/Ugbot/Agentic-Streaming/security/advisories/new

That form creates a draft security advisory that only the maintainer and the reporter can see.
It depends on the repository setting "Private vulnerability reporting" (Settings, Code
security and analysis); if GitHub answers that the form is not available, open a regular
issue that says only that you have a security report and how the maintainer can contact you,
without any technical detail, and the maintainer will open the advisory from their side.

Include the affected module (the Flink framework, `ports/jagentic-core`, `agentic-pekko`,
`agentic-clj`, one of the Python packages, the A2A gateway, the tool services, a compose
stack), the commit you tested, and the steps or input that reproduce the problem. A minimal
workflow document or fixture that triggers the issue is the most useful reproduction.

You should receive an acknowledgement within seven days. The fix is developed in the advisory's
private fork, merged to `main`, and the advisory is published together with the fix. Credit is
given to the reporter unless they ask otherwise. There is no bug bounty.

## Scope

In scope: any code in this repository that is reachable from an untrusted input, in
particular

- the network-facing surfaces: the Quarkus A2A gateway (`a2a-gateway/`), the Quarkus tool
  services (`tool-services/`), the Pekko HTTP front door (`agentic-pekko`) and the FastAPI
  gateway (`ports/experimental/gateway-fastapi/`);
- workflow document loading and validation in every runtime (`agentic/v1` documents are
  data, and a hostile document must fail validation rather than execute anything);
- outbound HTTP made on behalf of model output or request bodies (web tools, crawlers, A2A
  peers, push notification webhooks);
- the compose stacks in the repository root and the scripts under `examples-bin/`.

Out of scope: vulnerabilities in third-party dependencies that are not exploitable through
this code (report those upstream; a dependency bump here is welcome as a pull request),
issues that require the development-only overrides listed below, and the experimental engine
adapters under `ports/experimental/` unless the report shows a path from a supported surface.

## Security posture

`docs/security.md` is the normative description of the security model; this section
summarises it so a reporter can tell expected behavior from a bug.

- Every HTTP, SSE, gRPC and MCP entry point of the A2A gateway, the tool services and the
  FastAPI gateway requires a shared bearer token (`Authorization: Bearer <token>`), compared
  in constant time. A missing, malformed, empty or
  unknown token is rejected with HTTP 401 (gRPC `UNAUTHENTICATED`). Health probes and the
  public A2A Agent Card are the only unauthenticated endpoints.
- Services fail closed: with no token configured, every protected request is rejected. The
  only way to run without a token is an explicit development override
  (`a2a.auth.dev.mode`, `TOOL_SERVICES_AUTH_DEV_MODE`, `AGENTIC_GATEWAY_AUTH_DEV_MODE`).
- Caller identity comes only from the validated token. A2A tasks record their owner, and
  `tasks/get`, `tasks/cancel` and the push notification config methods refuse a caller that is
  not the owner. The FastAPI gateway scopes conversations per principal.
- Outbound URLs that originate from untrusted input pass through `OutboundUrlPolicy`:
  `http`/`https` only, no userinfo, every resolved address must be public (loopback,
  link-local, private ranges, the cloud metadata address, multicast, unique-local and
  IPv4-in-IPv6 forms are rejected), optional host allowlists, manual redirect handling with
  each hop re-validated and a cap on the chain, and timeouts always set.
- Agents may only call tools on their declared allowlist; the JVM `LlmBrain` rejects a tool
  call the agent did not declare as a validation error.
- Compose stacks publish ports on `127.0.0.1` only, carry no committed credentials, and
  refuse to start until the required passwords and tokens are set in `.env`. Provider API
  keys are read from the environment only.
- The Flink JobManager REST API in the compose stacks has no authentication and must not be
  exposed beyond the host.

Anything that contradicts one of these statements on `main` is a vulnerability and should be
reported through the channel above. The exact settings, environment variables and code
locations are listed in `docs/security.md`.
