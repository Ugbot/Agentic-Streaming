# Agentic Clojure: a first-class, pure-Clojure agent runtime on Datomic

A complete, idiomatic Clojure realization of the agent essence. **not** a JVM-interop wrapper around
the Java core. The whole pipeline (router → path → verifier, tools, RAG, guardrails, LLM brains, the
declarative loader) is reimplemented in plain Clojure data + protocols + functions, with **Datomic**
as the first-class storage engine: each message is an immutable datom, so the conversation transcript
*is* an event log with time-travel (`d/history` / `as-of`) for free. A peer to the Flink framework and
Agentic Pekko, at byte-for-byte parity with the other cores (the FNV embedder + banking goldens).

## Why Clojure + Datomic fits the essence

The essence is *an agent's state is a materialized view over an ordered, immutable log of events.*
That is precisely Datomic's data model: facts (datoms) are never updated in place, they accumulate,
and any past state is a query against a database value `as-of` a point in time. Modelling the
transcript as datoms means the "event-sourced agent" is the natural shape, not a bolt-on. Clojure's
values-and-functions style maps the rest cleanly: brains/routers/verifiers are just functions,
the tool registry is a map, the turn pipeline is a pure transformation over a context map.

## Architecture

| Namespace | What |
|-----------|------|
| `agentic.event` / `agentic.context` | The inbound message map `{:conversation-id :user-id :text :metadata}` and the per-turn context `{:store :state :tools :retriever :listeners ...}`. |
| `agentic.graph` | `handle`, the turn pipeline (input guardrails → router → path brain → verifier → output guardrails → listeners), reproducing jagentic-core's `RoutedGraph.handle` exactly, writing `phase`/`path` attributes at each step. |
| `agentic.brain` | `keyword-brain` (tool-trigger / retrieval / echo), the generic rule brain. |
| `agentic.llm` | `ChatClient` protocol + `stub-chat-client` (offline) + `ollama`/`openai` clients (clj-http); `llm-brain`, the JSON-mode ReAct loop (`{"tool":..,"args":..}` \| `{"text":..}`) mirroring `LlmBrain`. |
| `agentic.tools` | The tool registry, an atom of `id -> {:description :schema :fn}`; `register` / `execute` / `specs` / `tool-descriptors`. |
| `agentic.retrieval` | The **FNV-1a hashing embedder** with the exact cross-core constants, `cosine`, and the two-tier (hot+cold) retriever, vectors are byte-identical to the Python/Java/Go cores. |
| `agentic.guardrail` | `regex-guardrail` + `classifier-guardrail` (lexicon) → `{:check-input :check-output}`. |
| `agentic.saga` / `agentic.context-window` | Reverse-order compensation; MoSCoW context compaction. |
| `agentic.store` | The `ConversationStore` / `KeyedStateStore` / `LongTermStore` protocols + atom-backed in-memory impls (the model-free default). |
| `agentic.store.datomic` | The same three protocols reified over `datomic.client.api` (datalog + `transact`); composite unique identities for upsert. The first-class storage engine. |
| `agentic.pipeline` | `build` (spec map → `{:graph :tools :retriever}`) + `load-system`, the **same** declarative schema as the other cores, as EDN (native) or YAML (clj-yaml). `banking.yaml` loads and runs unchanged. |
| `agentic.banking` | The worked example. KB, router rules, rule-brain, `get_balance` (1234.56), reproducing the shared goldens. |
| `agentic.http` | http-kit front door: Agent Card + `POST /agent`. A2A-interoperable. |
| `agentic.mcp` | JSON-RPC 2.0 stdio server over the tool registry (`initialize` / `tools/list` / `tools/call`). Clojure tools callable by any MCP client. |

## Datomic as the event log

`agentic.store.datomic/datomic-stores` opens a connection, ensures the database, and transacts the
schema (idempotent, upsert by `:db/ident`, so many app instances can share one external database).
Messages are appended as immutable datoms keyed by `conversation/id` + position;
attributes/keyed-state/facts upsert via composite unique identities (`cid|key`, `cid|name`,
`uid|key`). Because nothing is mutated, the full transcript history and any prior state are
recoverable by querying the db value at a past basis-t.

### One codebase, three deployments

The **same `datomic.client.api`** code runs against all three, selected purely by config
(`agentic.store.datomic/client-config` resolves the client map; see its docstring):

| Deployment | `:server-type` | Config keys | Notes |
|---|---|---|---|
| In-process (`com.datomic/local`) | `:datomic-local` | `:system`, `:storage-dir` (`:mem` or a dir) | the default, dev/test, no server |
| **Datomic Pro** (external Peer Server) | `:peer-server` | `:endpoint`, `:access-key`, `:secret`, `:validate-hostnames` | DB provisioned out of band, so `create-database` is skipped |
| **Datomic Cloud** | `:cloud` | `:region`, `:system`, `:endpoint` | |

```clojure
;; in-process (default)
(dat/datomic-stores {:storage-dir :mem :db-name "agentic"})

;; external Datomic Pro peer server
(dat/datomic-stores {:server-type :peer-server
                     :endpoint "localhost:8998" :access-key "k" :secret "s"
                     :validate-hostnames false :db-name "agentic"})

;; or hand a full client map straight through
(dat/datomic-stores {:client {:server-type :cloud :region "us-east-1" :system "prod"
                              :endpoint "https://...execute-api...amazonaws.com"}
                     :db-name "agentic"})
```

The same selection works from a `pipeline.yaml` `stores` section:

```yaml
stores:
  conversation:
    kind: datomic
    server-type: peer-server
    endpoint: localhost:8998
    access-key: ${DATOMIC_ACCESS_KEY}
    secret: ${DATOMIC_SECRET}
    validate-hostnames: false
    db-name: agentic
```

A live peer-server round-trip test runs when `AGENTIC_DATOMIC_ENDPOINT` (+ `_ACCESS_KEY` / `_SECRET`
/ `_DB`) is set, and skips cleanly otherwise; the file-backed persist-and-reconnect test always runs.

## Run it

Requires the [Clojure CLI](https://clojure.org/guides/install_clojure) (tools.deps).

```bash
clojure -X:test          # the full suite (23 tests / 96 assertions)
clojure -M:run           # the banking demo - a multi-turn conversation with persisted state
clojure -M:http          # HTTP front door on :8080  (GET /.well-known/agent-card.json, POST /agent)
clojure -M:mcp           # MCP stdio server over the tool registry
clojure -M:time-travel   # Datomic transcript time-travel - replay the conversation `as-of` an earlier point
```

## Packaging

`build.clj` (tools.build, alias `:build`) turns this module into a library artifact with the
coordinates `io.github.ugbot/agentic-clj`:

```bash
clojure -T:build version   # print the version the next two commands use
clojure -T:build jar       # target/agentic-clj-<version>.jar with its pom (also copied to target/pom.xml)
clojure -T:build install   # the same jar and pom into the local ~/.m2 repository
clojure -T:build clean     # remove target/
```

The version is not written anywhere in this directory. `build/agentic/build/version.clj` derives it
from the repository's release tags with the rule setuptools-scm applies to the Python distributions
(`tools/check_release_version.py` at the repository root), so one tag names one version across
every artifact: a checkout exactly at `v1.0.0rc1` builds `1.0.0rc1`; three commits after it build
`1.0.0rc2.dev3`; a checkout with no `v*` tag builds `0.1.dev<commit count>`. A tag that is not `v` plus
a canonical PEP 440 version fails the build. `test/agentic/build_version_test.clj` pins these rules.

The pom lists the `:deps` of `deps.edn` (aliases excluded), the Apache-2.0 license, and the
`<scm>` block with the tag when the build is exactly at one, otherwise the commit. A consumer
resolves an installed build with `{:deps {io.github.ugbot/agentic-clj {:mvn/version "<version>"}}}`.
Nothing in `build.clj` talks to Clojars; the publishing steps are written down, and deliberately
not automated, in `docs/release.md` at the repository root.

### Time-travel over the transcript

Because every message is an immutable datom, any past state of a conversation is just a query `as-of`
a point in the log, no event-replay machinery. `clojure -M:time-travel` runs a multi-turn banking
conversation, captures the basis-`t` after turn 1, keeps talking, then replays the transcript exactly
as it stood back then (a strict prefix of the current one). The helpers are
`agentic.store.datomic/basis-t` and `history-as-of`.

`POST /agent` example:

```bash
curl -s localhost:8080/agent \
  -H 'content-type: application/json' \
  -d '{"conversation_id":"c1","user_id":"u","text":"what is my balance?"}'
# {"conversation_id":"c1","reply":"[payments] ... 1234.56 ...","path":"payments","ok":true,"tool_calls":["get_balance"]}
```

## Parity

`agentic.banking` reproduces the goldens the other cores assert; the test suite checks the FNV
embedder produces byte-identical vectors (offset `0x811C9DC5`, prime `0x01000193`, 32-bit mask, token
regex `[a-z0-9]+` lowercased, dim 256, L2-normalized) and that the shared `banking.yaml` routes
identically (payments/cards/general + the regex guardrail block). The Datomic store round-trips
(append/history/attributes/keyed/long-term + the user index) run on an in-`mem:` database and skip
cleanly if Datomic can't be resolved in the environment.

## Agentic v1 conformance

This module is a v1 runtime for the language-neutral contract in `spec/`:

- `agentic.spec` reads a workflow as YAML, JSON or EDN. EDN uses kebab-case keywords
  (`:spec-version`, `:tool-triggers`, `:x-owner`) per the mapping in `spec/README.md`; every form is
  canonicalized to that shape, validated against `spec/v1/workflow.schema.json` (read from the repo,
  never copied) with the schema's defaults applied, and then checked against the seven loader rules.
  Unknown keys outside `x-` and `runtime:` are a `:validation` error with the offending key path.
- `agentic.log` is the closed event-type set, the dense per-conversation sequence, the state fold
  (`turn-count`, `transcript-length`, `last-retrieved-ids`) and the normalized result derived from a
  turn's events (`->wire` gives the `spec/v1/result.schema.json` shape). Unknown event types survive
  replay and are ignored by the fold.
- `agentic.core` gives each conversation one mailbox (a Clojure agent): deliveries are processed one at
  a time in arrival order, `turn_id` is the idempotency key (a redelivery answers `duplicate` from
  the log, appending nothing), and a `signal` resumes a suspended turn — including after a restart,
  because the pending suspension is rebuilt from the log.
- `agentic.context/call-tool` numbers attempts under `policies.retry`, emitting `tool_failed` per failed
  attempt and `tool_called`/`delegated` on success with a dense per-turn `index`; `agentic.graph`
  bounds verification by `policies.verification` and runs `saga.steps` in order, compensating the
  completed steps in reverse as `compensation_step` events.
- `agentic.store.datomic` appends transcript positions and event sequences through a unique
  `<conversation>|<position>` key and retries on `:db.error/unique-conflict`, so concurrent writers
  produce a gap-free log instead of racing a read-modify-write.
- `backend` must be one this module runs (`local`, `clojure`, `datomic`); `stores.conversation.kind`
  selects `memory` or `datomic`, and an unreachable Datomic fails the load unless the section says
  `on_unavailable: degrade`.

`agentic.conformance` binds the shared fixtures in `spec/conformance/v1/fixtures/*.yaml` directly:
it builds each workflow, delivers the turns (honouring `restart_runtime`, `signal`, `concurrent_with`),
validates every normalized result against the result schema and applies the comparison rules from
`spec/conformance/v1/README.md`. A fixture whose `requires` this runtime does not claim is recorded
as a skip. `clojure -X:test` runs it with the rest of the suite; from a REPL,
`(agentic.conformance/report (agentic.conformance/run-all))` prints one line per fixture.

## Model-free by default

The default brains are rule-based and the default embedder is the deterministic FNV hasher, so the
offline suite is fully green with no network or API keys. Real LLMs/embeddings are opt-in via the
`ollama`/`openai` `ChatClient`s (clj-http), point a pipeline's `llm`/`embeddings` section at a live
provider to use them.
