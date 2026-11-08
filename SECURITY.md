# Security policy

## Supported versions

This project is experimental and pre-release. Nothing is published: there are no releases and no
artifacts on Maven Central or PyPI. The only supported version is the `main` branch of
https://github.com/Ugbot/Agentic-Streaming. Security fixes land on `main`; there are no
maintenance branches to backport to.

| Version | Supported |
|---------|-----------|
| `main`  | yes       |
| anything else (forks, local builds of older commits) | no |

## Reporting a vulnerability

Do not put technical detail of a security problem in a public issue.

GitHub's private vulnerability reporting is switched off for this repository today (the API answered
`enabled: false` on 2026-11-07), so the advisory form does not work yet. Until it is switched on:

1. Open a regular issue titled "Security report" that says only that you have one and which module it
   is in. No detail, no reproduction.
2. The maintainer will open a draft security advisory and invite you to it. The advisory and its
   private fork are visible only to the two of you, and the detail goes there.

Once the setting is on, use https://github.com/Ugbot/Agentic-Streaming/security/advisories/new directly.

Include the affected module (the Flink framework, `ports/jagentic-core`, `agentic-pekko`, one of
the Python packages, a compose stack), the commit you tested, and the steps or input that reproduce the problem. A minimal
workflow document or fixture that triggers the issue is the most useful reproduction.

You should receive an acknowledgement within seven days. The fix is developed in the advisory's
private fork, merged to `main`, and the advisory is published together with the fix. Credit is
given to the reporter unless they ask otherwise. There is no bug bounty.

## Scope

In scope: any code in this repository that is reachable from an untrusted input. Read
`docs/security.md` first: much of what you might report there is a known gap, not a surprise.

Out of scope: vulnerabilities in third-party dependencies that are not exploitable through
this code (report those upstream; a dependency bump here is welcome as a pull request).

## Trust model

- **A workflow document is trusted input. Treat it as code.** It chooses the tools, the URLs, the
  prompts and the policy. Validation rejects a malformed document; it does not make a hostile one
  safe, so load only documents you wrote or reviewed.
- **A document cannot widen what the operator set.** `mcp[].command` starts a process, so it runs only
  if the operator listed it in `AGENTIC_MCP_ALLOWED_COMMANDS`, and with no list nothing starts. The
  same goes for the egress settings. These are environment variables on the host, not document keys.
- **Text is not trusted.** User text, tool results, retrieved passages and model output can say
  anything. They cannot grant a tool: that is the tool policy, and it is not a filter.
- **Whoever delivers an event vouches for its principal,** unless the runtime has the gateway's
  public keys (`AGENTIC_PRINCIPAL_KEYS`), when only a principal the gateway signed for that turn is
  believed. In the unauthenticated mode anyone who can write to the input topic can claim any
  identity. Kafka ACLs on that topic are required in both.

## Security posture

`docs/security.md` states what the code does today. The project is experimental and has had no
outside security review, so do not expose it to untrusted callers.
