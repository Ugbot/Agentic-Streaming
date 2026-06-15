(ns agentic.timers
  "Phase 2: portable timers — the counterpart of Flink's TimerService / a Pekko scheduler / a Temporal
   timer. Time is *logical*: callers advance the clock with (advance-to now) and get back the due timers,
   so tests are deterministic; a real-time driver simply calls (advance-to now) on a tick. Powers SLAs,
   escalate-after-N, retries, scheduled follow-ups, and CEP `within` expiry. Mirror of jagentic-core's
   org.jagentic.core.timers.* — behaviour at parity, expressed idiomatically.

   A timer is a map {:id :fire-at :payload} where :payload is an event map (agentic.event)."
  (:require [clojure.data.json :as json]
            [agentic.store :as store]))

;; ---- TimerService protocol ----

(defprotocol TimerService
  (schedule [_ id fire-at payload]
    "Schedule (or REPLACE by id) a timer to fire `payload` at `fire-at`. A replaced timer takes the
     new schedule order.")
  (cancel [_ id]
    "Cancel a pending timer; returns true if one was removed, else false.")
  (advance-to [_ now]
    "Remove and return all timers due at `now` (:fire-at <= now), ascending by :fire-at with schedule
     order as the stable tie-break.")
  (next-deadline [_]
    "The earliest pending :fire-at, or nil if no timers are pending."))

;; ---- in-memory: insertion-ordered map, atom of {:order [ids...] :by-id {id -> timer}} ----

(defn- om-remove
  "Drop id from the ordered map (no-op if absent)."
  [om id]
  (if (contains? (:by-id om) id)
    {:order (filterv #(not= % id) (:order om))
     :by-id (dissoc (:by-id om) id)}
    om))

(defn- om-schedule
  "Schedule/replace: re-insert at the end so a replaced timer takes the new schedule order."
  [om id fire-at payload]
  (let [base (om-remove om id)]
    {:order (conj (:order base) id)
     :by-id (assoc (:by-id base) id {:id id :fire-at fire-at :payload payload})}))

(defn- om-ordered-timers
  "Pending timers in schedule (insertion) order."
  [om]
  (mapv (:by-id om) (:order om)))

(defn- om-due
  "Timers with :fire-at <= now, ascending by :fire-at, schedule order as the stable tie-break.
   `sort-by` is stable, so equal deadlines keep insertion order."
  [om now]
  (->> (om-ordered-timers om)
       (filter #(<= (:fire-at %) now))
       (sort-by :fire-at)
       vec))

(deftype InMemoryTimerService [!state]
  TimerService
  (schedule [_ id fire-at payload]
    (swap! !state om-schedule id fire-at payload)
    nil)
  (cancel [_ id]
    ;; Atomically remove and report whether it was present, via swap-vals!.
    (let [[old _] (swap-vals! !state om-remove id)]
      (contains? (:by-id old) id)))
  (advance-to [_ now]
    ;; Compute due against the pre-swap state, then remove exactly those ids atomically.
    (let [[old _] (swap-vals! !state
                              (fn [om]
                                (reduce (fn [acc t] (om-remove acc (:id t)))
                                        om (om-due om now))))]
      (om-due old now)))
  (next-deadline [_]
    (let [ts (om-ordered-timers @!state)]
      (when (seq ts) (reduce min (map :fire-at ts))))))

(defn in-memory-timer-service
  "Process-local timers backed by an atom holding an insertion-ordered map. Due timers come out
   ascending by :fire-at with schedule order as the stable tie-break."
  []
  (->InMemoryTimerService (atom {:order [] :by-id {}})))

;; ---- durable: in-memory delegate + a KeyedStateStore slot ----

;; Serialization is per-process self-consistent (never read across languages), so each core encodes
;; idiomatically. Here: a JSON vector of {:id :fire-at :conversation-id :user-id :text}. Parity is the
;; behaviour — "a scheduled timer survives restore and fires" — not the wire format.

(defn- encode-pending
  [timers]
  (json/write-str
   (mapv (fn [{:keys [id fire-at payload]}]
           {:id id
            :fire-at fire-at
            :conversation-id (:conversation-id payload)
            :user-id (:user-id payload)
            :text (:text payload)})
         timers)))

(defn- decode-pending
  [raw]
  (->> (json/read-str raw :key-fn keyword)
       (mapv (fn [{:keys [id fire-at conversation-id user-id text]}]
               {:id id
                :fire-at fire-at
                :payload {:conversation-id conversation-id
                          :user-id user-id
                          :text text
                          :metadata {}}}))))

(declare persist!)

(deftype DurableTimerService [delegate store slot-key slot-name]
  TimerService
  (schedule [this id fire-at payload]
    (schedule delegate id fire-at payload)
    (persist! this)
    nil)
  (cancel [this id]
    (let [removed (cancel delegate id)]
      (when removed (persist! this))
      removed))
  (advance-to [this now]
    (let [due (advance-to delegate now)]
      (when (seq due) (persist! this))
      due))
  (next-deadline [_] (next-deadline delegate)))

;; persist!/restore are declared after the type so they can dispatch on its fields.
(defn- persist!
  [^DurableTimerService dts]
  (let [delegate (.-delegate dts)]
    (store/kv-put (.-store dts) (.-slot-key dts) (.-slot-name dts)
                  (encode-pending (om-ordered-timers @(.-!state delegate))))))

(defn restore
  "Reload pending timers from the store into this service (call after a restart). Reschedules each
   persisted timer through the delegate, preserving order."
  [^DurableTimerService dts]
  (let [raw (store/kv-get (.-store dts) (.-slot-key dts) (.-slot-name dts))]
    (when raw
      (let [delegate (.-delegate dts)]
        (doseq [{:keys [id fire-at payload]} (decode-pending raw)]
          (schedule delegate id fire-at payload))))
    dts))

(defn durable-timer-service
  "A TimerService that persists its pending set through a KeyedStateStore (agentic.store), so timers
   survive a restart with a durable store backing. Pending timers are written to one slot as JSON;
   (restore) reloads them. Schedule/cancel/advance keep the slot current."
  ([store] (durable-timer-service store "__timers__" "pending"))
  ([store slot-key slot-name]
   (->DurableTimerService (in-memory-timer-service) store slot-key slot-name)))
