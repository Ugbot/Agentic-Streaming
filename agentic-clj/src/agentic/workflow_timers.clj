(ns agentic.workflow-timers
  "Workflow `timers` (spec/v1/primitives.md, section 8) and the two clocks they read. Processing time
   is a logical clock owned by the system: `system-clock` reads wall time, `manual-clock` moves only
   when told to (`advance!`) and never backwards, which is how a conformance fixture's
   `advance_time_ms` drives it. Event time is the conversation watermark, the highest `event_time_ms`
   turn metadata seen, folded from the log.

   Pending timers are never held in memory across turns: `fold` derives them from `timer_scheduled`
   minus `timer_fired`, so a restarted runtime recovers exactly the timers still owed and can neither
   re-schedule nor double-fire one. Every turn_received of a workflow with timers records the
   processing clock reading (`processing_time_ms`), so the clock itself is recoverable from the log."
  (:require [agentic.cep-fold :as cep-fold]
            [agentic.context :as ctx]))

;; ---- clocks ----

(defprotocol Clock
  (now-ms [_] "The current processing time in milliseconds."))

(defrecord SystemClock []
  Clock
  (now-ms [_] (System/currentTimeMillis)))

(defrecord ManualClock [!now]
  Clock
  (now-ms [_] @!now))

(defn system-clock [] (->SystemClock))

(defn manual-clock
  "A logical processing clock starting at `start-ms` (default 0)."
  ([] (manual-clock 0))
  ([start-ms]
   (when (neg? start-ms)
     (throw (ex-info (str "logical time cannot be negative: " start-ms) {:error/class :fatal})))
   (->ManualClock (atom (long start-ms)))))

(defn advance!
  "Move a manual clock forward by `ms`; logical time never moves backwards."
  [^ManualClock clock ms]
  (when (neg? ms)
    (throw (ex-info (str "logical time never moves backwards: advance by " ms) {:error/class :fatal})))
  (swap! (:!now clock) + (long ms))
  clock)

;; ---- the fold ----

(def empty-state
  {:watermark-ms nil :processing-time-ms nil :pending {} :fired []})

(defn fold
  "Timer state as a fold over a conversation's events: the watermark, the last recorded processing
   clock reading, the pending timers by id and the fired ids in firing order."
  [events]
  (reduce (fn [state {:keys [type payload]}]
            (case type
              :turn-received
              (cond-> state
                (some? (:event-time-ms payload))
                (update :watermark-ms #(max (or % (:event-time-ms payload)) (:event-time-ms payload)))
                (some? (:processing-time-ms payload))
                (assoc :processing-time-ms (:processing-time-ms payload)))
              :timer-scheduled
              (assoc-in state [:pending (:timer-id payload)]
                        {:timer-id (:timer-id payload) :clock (:clock payload) :due-ms (:due-ms payload)})
              :timer-fired
              (if (contains? (:pending state) (:timer-id payload))
                (-> state
                    (update :pending dissoc (:timer-id payload))
                    (update :fired conj (:timer-id payload)))
                state)
              state))
          empty-state
          events))

(defn recovered-processing-time
  "The processing clock reading a restarted runtime rebuilds from its logs: the highest recorded
   `processing_time_ms`, or 0 when no turn recorded one."
  [event-logs]
  (reduce max 0 (keep #(:processing-time-ms (fold %)) event-logs)))

;; ---- turns ----

(defn event-time-of
  "The turn's `event_time_ms` metadata as a long, or nil when the turn carries none; the same read
   the CEP fold uses (`agentic.cep-fold/event-time-ms`), so timers and patterns agree on event time."
  [event]
  (cep-fold/event-time-ms (:metadata event)))

(defn watermark-after
  "The watermark once a turn with `event-time` (or nil) has arrived: never lower than before."
  [state event-time]
  (cond (nil? event-time) (:watermark-ms state)
        (nil? (:watermark-ms state)) event-time
        :else (max (:watermark-ms state) event-time)))

(defn- reading
  "The clock a timer reads: processing time from the system clock, event time from the watermark
   (0 until the conversation has seen an event time)."
  [clock processing-now watermark]
  (if (= "event" clock) (or watermark 0) processing-now))

(defn- active?
  "Timers only take part in a turn when the workflow declares some and the context carries a clock
   and the conversation's prior events (`:clock`, `:prior-events`, set by agentic.core)."
  [graph c]
  (and (seq (:timers graph)) (some? (:clock c))))

(defn processing-now
  "The processing clock reading for this turn, or nil when the workflow declares no timers."
  [graph c]
  (when (active? graph c) (now-ms (:clock c))))

(defn received-payload
  "The `turn_received` payload with the event time and, for a workflow with timers, the processing
   clock reading this turn was processed at."
  [graph event c payload]
  (let [event-time (event-time-of event)
        now (processing-now graph c)]
    (cond-> payload
      (some? event-time) (assoc :event-time-ms event-time)
      (some? now) (assoc :processing-time-ms now))))

(defn fire-due!
  "Before a later turn is received: fire every pending timer whose clock reads at or past its
   deadline, ordered by (due_ms, timer_id), appending `timer_fired` and calling the timer's tool
   with its declared payload. The first turn of a conversation fires nothing."
  [graph event c]
  (when (active? graph c)
    (let [state (fold (:prior-events c))
          first-turn? (empty? (:prior-events c))
          now (now-ms (:clock c))
          watermark (watermark-after state (event-time-of event))]
      (when-not first-turn?
        (doseq [{:keys [timer-id due-ms]}
                (->> (vals (:pending state))
                     (filter (fn [t] (>= (reading (:clock t) now watermark) (:due-ms t))))
                     (sort-by (juxt :due-ms :timer-id)))]
          (ctx/emit! c :timer-fired {:timer-id timer-id :due-ms due-ms})
          (let [spec (some #(when (= timer-id (:id %)) %) (:timers graph))]
            (when (:tool spec)
              (ctx/call-tool c (:tool spec) (or (:payload spec) {})))))))))

(defn schedule!
  "After the first turn of a conversation is received: append `timer_scheduled` for every declared
   timer, due `after_ms` past its clock's current reading."
  [graph event c]
  (when (and (active? graph c) (empty? (:prior-events c)))
    (let [state (fold (:prior-events c))
          now (now-ms (:clock c))
          watermark (watermark-after state (event-time-of event))]
      (doseq [{:keys [id after-ms clock]} (:timers graph)]
        (let [clock (or clock "processing")]
          (ctx/emit! c :timer-scheduled {:timer-id id :clock clock
                                         :due-ms (+ (reading clock now watermark) (long after-ms))}))))))
