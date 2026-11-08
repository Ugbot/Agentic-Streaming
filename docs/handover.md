# Handover, 8 November 2026

Where the cornerstone work on Agentic Streaming stands. **`main` is unchanged** (`11e52c2`).
Everything below is on branches, waiting for a maintainer to review and merge.

## The aim
Make the project safe, secure and working as intended: narrow it to Apache Flink (primary), plain
Python and Apache Pekko on the Java core and reference spec; mark it experimental; move experimental
code out; fix every known vulnerability; then keep going through the plan. The guarantee it works
towards:

> State is exactly-once. Effects are at-least-once under a stable key. Anything that cannot take a
> key is an explicit human decision.

## The pull requests, in order
| PR | Branch | Holds | Verified |
|---|---|---|---|
| #99 | `chore/move-experimental-out` | Narrow to Flink, Python and Pekko; mark experimental; move experimental code out | CI green |
| #101 | `fix/vulnerabilities` | Dependency fixes; Fluss and Milvus opt-in | OSV clean on resolved trees; integration tests on Docker |
| #102 | `fix/phase-1-stop-the-crashes` | Crashes, timeouts, real models from YAML, a Kafka pipeline on a real cluster | 3 clean full Java runs |
| #103 | `chore/remove-legacy-dsl` | Legacy DSL removed: Flink main code from 61,600 to 3,540 lines | CI green |
| #104 | `feat/effect-journal` | Crash fixtures and the effect journal | Self-reported, then attacked in #105 |
| #105 | `next/combined` | #99 to #104 combined, plus 17 fixes from an adversarial pass | 76 of 76 fixtures on 5 runtimes |
| #106 | `feat/auth-and-control` | Identity, auth and control | Self-reported, then attacked in #107 |
| #107 | `review/auth-attack` | Security review: 3 high and several medium fixes | 111 of 111 fixtures on 5 runtimes, one run |

**Simplest way to land it:** retarget #105 to `main`, approve and squash-merge it. It contains #99 to
#104, and squashing hides two commits that don't build on their own. Then #106, then #107, then this
branch. Close the superseded PRs afterwards.

## This branch: `fix/security-open-findings`
Based on #107. No PR yet.

- **Done and pushed:** conversations bound to their first subject as well as tenant; DNS rebinding
  closed in Java and Python (resolve once, check every address, connect to the checked one); MCP
  tool descriptions marked untrusted and length-capped; secret references restricted to an operator
  allowlist; RFC 9068 `at+jwt` tokens accepted; a refused signal logged once per sender and turn;
  limit counters pruned.
- **Not yet committed:** the gateway-signed principal, which fixes the one open high finding. Work in
  progress in `ports/jagentic-core` (`identity/`, `Admission`, `Event`, `RoutedGraph`,
  `GraphBuilder`). It must be finished, tested and committed before this branch is reviewed.
- **Not started:** erasure reaching the results topics and the gateway's view built from them.

**Until the signed principal lands, anyone who can write the Kafka input topic can claim any
identity. Kafka ACLs on that topic are required.**

## Known gaps
- A crash during a call to a `non_idempotent` tool leaves the outcome unknown; the turn waits for a
  person. That is by design.
- Two live Kafka journal writers are not excluded. A rollback plus a due timer ends the turn fatal.
- Limits are per Flink subtask, so not a hard ceiling. Crypto-shredding does not yet cover Pekko's
  log, Python or the results topics.
- Python has no HTTP front door. Its CLI runs all seven examples on the core; it refuses the stores
  and embedders the JVM has and it does not (`feat/python-parity`).
- Flink lost HTTP MCP with the legacy DSL, and ignores `stores:`.
- Untested: any live model, `KafkaBackend` against a live broker, the Postgres journal on a session
  cluster, Milvus against a server, MCP OAuth, A2A 1.0.1.
- One restart test failed once under heavy load and has not recurred.

## Not started
Observability and lineage (OTel, OpenLineage, decision replay); the first release (Maven group
`com.aibywire`, PyPI `agentic-streaming`, signed); non-blocking Flink turns; parity work against
Apache Flink Agents.

## Decisions for the maintainer
- Merge the PRs as above.
- Update `CLAUDE.md` and `AGENTS.md`. They still describe the old DSL.
- Review two spec changes: undeclared tools now default to `non_idempotent`, and a transient re-run
  replays from the journal instead of calling again. Both change behaviour.
- Turn on GitHub private vulnerability reporting. It is off.

## How this work was done
- Each self-reported green result got an independent adversarial pass. Both times it found real
  bugs: 17 in the effect journal, and 3 high plus several medium in auth. Keep doing that.
- Every fix needs a test that failed before it.
- Push after every commit.
