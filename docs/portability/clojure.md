# Agentic Streaming in Clojure (on Datomic)

> Per the keystone [`00-essence-and-core-abstractions.md`](00-essence-and-core-abstractions.md).
> A **first-class** module lives in [`../../agentic-clj/`](../../agentic-clj/) (runs + tested:
> 16 tests / 64 assertions). Unlike the JVM *ports* (which reuse `jagentic-core`), this is a
> **pure-Clojure reimplementation** of the essence — no Java-core dependency.

## 1. Verdict

Clojure + Datomic is the most *philosophically* aligned realization in the series. The essence
states it outright — *an agent's state is a materialized view over an ordered, immutable log of
events* — and that sentence is a description of Datomic. Datoms accumulate and are never updated in
place; any past state is a query against the database value `as-of` a basis point. So "event-sourced
agent" isn't a pattern you build on top of the store — it's what the store *is*. Clojure supplies the
rest idiomatically: the turn pipeline is a pure function over a context map, brains/routers/verifiers
are plain functions, the tool registry is a map, values carry no ceremony. What you give up versus
Flink is a streaming-topology engine and native checkpointed keyed state — delivery here is the
online turn (HTTP / MCP / in-process), with Datomic providing durability and a single connection per
conversation providing ordering.

## 2. Capability mapping (C1..C12)

| Cap | How the Clojure module supplies it |
|-----|------------------------------------|
| **C1** durable keyed state | **N** — the `ConversationStore`/`KeyedStateStore` protocols over Datomic; state is datoms keyed by `conversation/id`. |
| **C2** per-key ordered processing | **L** — single-writer is a convention (one connection / serialized submit per conversation), not a partition guarantee; Datomic transactions are serializable. |
| **C3** fault tolerance / durability | **N** — Datomic durability (datoms persisted; `mem:` for tests, file / Pro / Cloud for prod); immutable history is recoverable. |
| **C4** async I/O | **L** — LLM/HTTP via clj-http (blocking) or `core.async` channels for the brain loop; not actor-async like Pekko. |
| **C5** backpressure | **L** — `core.async` buffered channels / the http-kit thread pool; no first-class stream backpressure. |
| **C6** connectors | **L** — clj-http for HTTP/LLM/A2A; Kafka via a client lib if wired. |
| **C7** side outputs | **L** — listeners + return data; plain function fan-out. |
| **C8** broadcast state | **L** — a shared atom / a Datomic value read by all turns. |
| **C9** event-time / windows | **—** — not a streaming-analytics engine. |
| **C10** CEP | **—** — out of scope; rule-brains cover routing. |
| **C11** distributed scale | **L** — Datomic peers/clients scale reads; turn execution is per-process (scale by running more processes behind a queue). |
| **C12** topology builder | **N** — `agentic.pipeline` builds the graph from the shared EDN/YAML spec. |

The standout: **C3 is native and free** because immutability is the storage model, and the
**time-travel** that other backends bolt on (replaying a journal to reconstruct state) is a plain
`as-of` query. Datomic is to storage what Pekko's event-sourced entity is to the actor — except you
don't replay to rebuild; the old value was never overwritten.

## 3. The core abstractions in Clojure

- **The turn (C1+C2).** `agentic.graph/handle` is a pure `(graph event ctx) -> turn-result` that runs
  the same sequence as `RoutedGraph.handle`: input guardrails → router → path brain → verifier →
  output guardrails → listeners, writing `phase`/`path` to the store as it goes.

  ```clojure
  (defn handle [graph event ctx]
    ;; input guardrails -> router -> (brain) -> verifier -> output guardrails -> listeners
    ;; ... writes :phase / :path attributes to the ConversationStore at each step
    {:conversation-id (:conversation-id event) :reply reply :path path :ok ok :tool-calls calls})
  ```

- **Brains / routers / verifiers are functions.** A brain is `(fn [user-text ctx] -> reply)`, a router
  `(fn [event ctx] -> path-key)`, a verifier `(fn [reply ctx] -> [ok? reply])`. `agentic.brain`
  ships the rule brain; `agentic.llm/llm-brain` is the ReAct loop over a `ChatClient` protocol.
- **Stores are protocols, Datomic is the impl.** `agentic.store` defines `ConversationStore` /
  `KeyedStateStore` / `LongTermStore` with atom-backed in-memory defaults; `agentic.store.datomic`
  reifies them over `datomic.client.api` (datalog `d/q` + `d/transact`). Each message is an immutable
  datom keyed by conversation id + position; attributes/keyed/facts upsert via composite unique
  identities. The **same code targets three deployments** by config (`client-config`): in-process
  `com.datomic/local` (`:datomic-local`), **external Datomic Pro** via a Peer Server (`:peer-server` +
  `:endpoint/:access-key/:secret`), and **Datomic Cloud** (`:cloud`). The schema transaction is
  idempotent, so many app instances safely share one external database; `create-database` is skipped
  for peer-server (provisioned out of band).
- **Retrieval (parity-critical).** `agentic.retrieval` reproduces the FNV-1a hashing embedder with
  the exact constants (offset `0x811C9DC5`, prime `0x01000193`, 32-bit mask, token regex
  `[a-z0-9]+` lowercased, dim 256, L2-normalize) so vectors are byte-identical to the other cores.
- **The pipeline.** `agentic.pipeline/build` + `load-system` accept the **same** declarative schema as
  the other cores — EDN (native) or YAML (clj-yaml with string keys) — so the shared `banking.yaml`
  loads and runs unchanged, with the `stores` section selecting `:datomic` vs in-memory.
- **Front doors.** `agentic.http` (http-kit) serves the Agent Card + `POST /agent`; `agentic.mcp`
  serves the tool registry over JSON-RPC 2.0 on stdio (`initialize`/`tools/list`/`tools/call`).

## 4. Worked example — banking router→path→verifier

`clojure -M:run` runs the banking demo (`agentic.main`): a multi-turn conversation where `c1`'s state
persists across turns in the store. The router sends balance queries to `payments` (firing the
`get_balance` tool → `1234.56`), card questions to `cards` (retrieval over the KB), everything else to
`general`; the regex guardrail blocks prompt-injection. These are the same goldens
`ParityTest`/`test_parity.py`/`parity_test.go` assert — the Clojure suite checks them too, proving
byte-for-byte agreement.

## 5. What doesn't fit

- **Streaming-analytics.** No event-time/windows/CEP engine — those stay Flink-first. The Clojure
  module is the *online agent*, not a stream processor.
- **Native partitioned single-writer.** C2 here is a convention (serialize submits per conversation),
  not a partition/shard guarantee like Flink keyBy or Pekko sharding. For strict single-writer at
  scale, front it with a per-conversation queue or run on Datomic with an external coordinator.
- **Blocking I/O.** clj-http calls block; for high concurrency move LLM calls onto `core.async` /
  a thread pool (the http-kit server already pools request threads).
- **Datomic licensing/footprint.** `com.datomic/local` is in-process and resolves from Maven Central;
  **Datomic Pro (peer server) and Cloud are the same client API**, selected by config (`:server-type`)
  with no code change — see the deployment table in the [module README](../../agentic-clj/README.md).
  The file-backed persist-and-reconnect test always runs; the live external (peer-server) round-trip
  runs when `AGENTIC_DATOMIC_ENDPOINT` is set and skips otherwise (the in-memory store is the
  model-free default).

## 6. When to choose Clojure

Choose the Clojure module when you want the agent essence in a **data-first, REPL-driven** stack where
**immutable history is the storage model** — Datomic gives you durable, queryable, time-travelling
conversation logs with no event-replay machinery, and the whole agent is plain functions over plain
data you can poke at the REPL. It's the natural home if your team already runs Clojure/Datomic. If you
need a partitioned streaming topology with exactly-once, that's Flink or Kafka Streams; if you want
actor-native clustering with event-sourced entities on the JVM, that's Agentic Pekko.
