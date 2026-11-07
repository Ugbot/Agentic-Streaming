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

Do not open a public issue for a security problem. Use GitHub's private vulnerability
reporting for this repository:

https://github.com/Ugbot/Agentic-Streaming/security/advisories/new

That form creates a draft security advisory that only the maintainer and the reporter can see.
It depends on the repository setting "Private vulnerability reporting" (Settings, Code
security and analysis); if GitHub answers that the form is not available, open a regular
issue that says only that you have a security report and how the maintainer can contact you,
without any technical detail, and the maintainer will open the advisory from their side.

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

## Security posture

`docs/security.md` states what the code does today. In short: the canonical path has no
authentication, the Pekko HTTP front door has none either, and workflow documents are trusted input that can start processes. Do not expose any of
this to untrusted callers.
