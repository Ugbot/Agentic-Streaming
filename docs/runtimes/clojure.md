# Clojure runtime

`agentic-clj` (directory `agentic-clj/`, `deps.edn`) is a pure Clojure implementation of
`agentic/v1` with Datomic as the storage engine. Unlike Flink, Pekko and the Python facade it
does not reuse `ports/jagentic-core`; the primitives are reimplemented in Clojure data,
protocols and functions, and the `clojure` column of the [matrix](../capabilities.md) measures
that implementation on its own. The module README (`agentic-clj/README.md`) has the namespace
table and the run aliases; this page is about how the primitives map.

## The log

`agentic.log` is the closed event type set, the dense per-conversation `sequence`, the state fold
(`turn-count`, `transcript-length`, `last-retrieved-ids`) and the normalized result derived from a
turn's events; `->wire` renders `spec/v1/result.schema.json`. Event types are kebab-case keywords
in Clojure (`:turn-received`) and snake_case on the wire. Unknown event types survive replay and
are ignored by the fold (`agentic.runtime-test/unknown-events-are-preserved-and-ignored-by-the-fold`).

## One agent per conversation

`agentic.core` gives each conversation one mailbox, a Clojure agent. Deliveries are processed one
at a time in arrival order; `turn_id` is the idempotency key and a redelivery answers `duplicate`
from the log, appending nothing (`single-writer-orders-concurrent-turns`,
`duplicate-turn-answers-from-the-log`). A `signal` resumes a suspended turn, including after a
restart, because the pending suspension is rebuilt from the log
(`suspended-turn-survives-restart-and-resumes-on-signal`).

## Datomic as the event log

`agentic.store.datomic` reifies the `ConversationStore`, `KeyedStateStore` and `LongTermStore`
protocols over `datomic.client.api`. Messages and events are appended as immutable datoms keyed
by conversation and position; attributes, keyed state and facts upsert through composite unique
identities. Concurrent writers append through a unique `<conversation>|<position>` key and retry
on `:db.error/unique-conflict`, so the log stays gap free instead of racing a read-modify-write.
The same code runs against in-process `com.datomic/local` (the default for dev and test), Datomic
Pro through a peer server, and Datomic Cloud, selected purely by configuration. `agentic.store`
provides atom-backed in-memory implementations of the same protocols for the model-free default.

## Time travel

Because nothing is mutated, any past state of a conversation is a query against the database
value `as-of` a point in the log. `agentic.store.datomic/basis-t` captures the current basis and
`history-as-of` reads the transcript as it stood there. `clojure -M:time-travel` runs a multi-turn
banking conversation, captures the basis after turn 1, keeps talking, then replays the transcript
exactly as it was, a strict prefix of the current one. `agentic.replay` is the log-driven
counterpart: `replay-until` stops at an as-of point over recorded events
(`agentic.replay-test/replay-until-stops-early-at-the-as-of-point`).

## Loader

`agentic.spec` reads a workflow as YAML, JSON or EDN (kebab-case keywords per `spec/README.md`),
validates it against `spec/v1/workflow.schema.json` read from the repository, applies the schema's
defaults and the loader rules, and rejects unknown keys outside `x-` and `runtime:` with the
offending path. `backend` must be `local`, `clojure` or `datomic`; `stores.conversation.kind`
selects `memory` or `datomic` (`loader-rejects-what-it-cannot-run`,
`edn-and-yaml-load-to-the-same-workflow`).

## Where it stands

<!-- matrix: clojure -->
Derived from [capabilities.md](../capabilities.md) (run 2026-09-14T17:15:17+00:00, commit `eef9c63ea3ff`) by
`docs/tools/matrix_excerpt.py`; do not edit by hand. Every capability not listed below is
[supported](../capabilities.md#capabilities) for the binding, meaning every fixture that requires it passed.

Binding `clojure`: 24 passed, 0 failed, 0 skipped.

| Capability | clojure |
|---|---|
<!-- /matrix -->

The three skipped fixtures are the spec's declared `timers`; the module declares no `timers`
capability, so they are recorded as skips, never passes ([clojure notes](../capabilities.md#clojure)).

## Running it

Requires the Clojure CLI (tools.deps). From `agentic-clj/`:

```bash
clojure -X:test          # the suite, including agentic.conformance/run-all, the clojure column
clojure -M:run           # banking demo
clojure -M:time-travel   # Datomic transcript time travel
clojure -M:http          # HTTP front door on :8080
```
