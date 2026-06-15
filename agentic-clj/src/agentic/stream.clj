(ns agentic.stream
  "Phase 1: the stream substrate — the portable realization of the project's thesis that an agent is a
   materialized view over a stream of events. A Channel is a pull-based event source; a stream-runtime
   drives a channel of events through agentic.core/submit as agent turns, in arrival order. Observers
   see each raw event before it becomes a turn — the seam CEP matchers, window aggregators and tracers
   plug into in later phases. Mirror of jagentic-core's org.jagentic.core.stream.*."
  (:require [agentic.core :as core]
            [agentic.timers :as timers]))

;; ---- Channel: a pull-based event source ----

(defprotocol Channel
  "A source of events — the portable counterpart of a Flink source / Kafka consumer / Pekko Source.
   Pull-based: (poll) returns the next available event, or nil when none is available right now. Never
   blocks. Bounded sources (a seed list) eventually return nil forever; unbounded sources (a queue) may
   return nil transiently and more later."
  (poll [_] "The next event if one is available, else nil. Never blocks."))

;; ---- seed-channel: bounded, replays a fixed seq then nil forever ----

(deftype SeedChannel [!remaining]
  Channel
  (poll [_]
    ;; Atomic pop-front: swap! returns the new state, so re-derive the popped head from the old state.
    (let [old (first (swap-vals! !remaining next))]
      (first old))))

(defn seed-channel
  "A bounded channel that replays the given events in order, then is nil forever."
  [events]
  (->SeedChannel (atom (seq events))))

;; ---- queue-channel: unbounded thread-safe FIFO ----

(deftype QueueChannel [!queue]
  Channel
  (poll [_]
    ;; Atomically pop the head: swap-vals! gives [old new]; the head is (peek old).
    (peek (first (swap-vals! !queue pop)))))

(defn queue-channel
  "An unbounded, thread-safe in-memory channel: producers (offer) events; the runtime polls them in
   FIFO order. A producer thread and the stream loop can share it safely."
  []
  (->QueueChannel (atom clojure.lang.PersistentQueue/EMPTY)))

(defn offer
  "Enqueue an event on a queue-channel; returns the channel so calls chain."
  [ch event]
  (swap! (.-!queue ^QueueChannel ch) conj event)
  ch)

;; ---- stream-runtime: drive a channel of events as agent turns ----

(defn stream-runtime
  "Build a stream-runtime over a `system` (what agentic.core/submit takes). Modelled as plain data —
   {:system :observers} — so observe/run are ordinary functions."
  [system]
  {:system system :observers []})

(defn observe
  "Register an observer of the raw event stream. An observer is a fn (fn [event] ...) called for each
   event before it becomes a turn. Returns the updated stream-runtime so calls chain."
  [sr observer-fn]
  (update sr :observers conj observer-fn))

(defn run
  "Drain every currently-available event from the channel as a turn, in arrival order, and return the
   vector of turn-results. Each observer sees the event first, then (core/submit system event) runs.
   Returns when the channel next reports nil (empty)."
  [{:keys [system observers]} channel]
  (loop [results (transient [])]
    (if-let [event (poll channel)]
      (do
        (doseq [obs observers] (obs event))
        (recur (conj! results (core/submit system event))))
      (persistent! results))))

(defn fire-due-timers
  "Fire every timer due at logical time `now` through the runtime, in deadline order. Pulls the due
   timers from `timer-service` via agentic.timers/advance-to (ascending by :fire-at, schedule order as
   the stable tie-break), and for each — exactly like (run) — lets observers see the payload event
   first, then submits it as a turn. Returns the vector of turn-results in deadline order. This is the
   seam SLAs / escalate-after-N / scheduled follow-ups fire on. `timer-service` must satisfy the
   agentic.timers/TimerService protocol."
  [{:keys [system observers]} timer-service now]
  (let [due (timers/advance-to timer-service now)]
    (mapv (fn [{:keys [payload]}]
            (doseq [obs observers] (obs payload))
            (core/submit system payload))
          due)))
