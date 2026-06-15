(ns agentic.replay
  "Phase 5: replay / time-travel — the portable form of the project's event-sourcing thesis. An agent's
   state is a materialized view over an ordered log of inbound events; persist that log and you can
   re-materialize state at will: replay through a FRESH runtime over the same graph reproduces the
   outcomes (determinism); replay through a runtime built on a NEW graph version answers \"what would the
   new prompts/routing have done\"; replay-until stops early to inspect the state as-of a point in the
   log (time-travel).

   Recording is free via the Phase-1 stream observer seam — (stream/observe sr (fn [e] (record log e)))
   captures each raw event before it becomes a turn. Mirror of jagentic-core's
   org.jagentic.core.replay.{EventLog,Replayer}.

   NOTE: this atom-backed event-log replay is the ENGINE-AGNOSTIC form of time-travel — it works over any
   agentic.core/submit runtime regardless of storage backend. When running on Datomic, the NATIVE
   equivalent is agentic.store.datomic/history-as-of, which gives as-of/history for free because each
   message is an immutable datom. No code dependency between the two — they are parallel realizations of
   the same idea."
  (:require [agentic.core :as core]))

;; ---- event-log: an append-only log of inbound events ----

(defn event-log
  "An append-only log of inbound events — the source of truth the agent's state is a materialized view
   over. Atom-backed: {:all (atom []) :by-key (atom {})}. Record events via (record log event); read them
   back in arrival order via (events log) or per-conversation via (events-for log cid)."
  []
  {:all (atom []) :by-key (atom {})})

(defn record
  "Append one event to the log, both globally and under its conversation-id. Returns the log so calls
   chain. Atom swaps are individually atomic — adequate for the single-writer recording seam."
  [log event]
  (swap! (:all log) conj event)
  (swap! (:by-key log) update (:conversation-id event) (fnil conj []) event)
  log)

(defn events
  "All recorded events, in arrival order, as a vector."
  [log]
  @(:all log))

(defn events-for
  "Recorded events for one conversation, in arrival order, as a vector."
  [log cid]
  (get @(:by-key log) cid []))

;; ---- replay: re-materialize state by submitting the events through a runtime ----

(defn replay
  "Submit each event to `system` in arrival order; return the vector of turn-results
   — (mapv #(core/submit system %) events). Use a FRESH system to reproduce outcomes (submit mutates
   store state), or a system over a new graph version to ask what that version would have done."
  [events system]
  (mapv #(core/submit system %) events))

(defn replay-until
  "Replay only the first `n` events — state as-of that point in the log. `n` is clamped to
   [0 (count events)]. (`n`, not `count`, because count is a core fn.)"
  [events n system]
  (let [n (max 0 (min n (count events)))]
    (replay (subvec (vec events) 0 n) system)))
