(ns agentic.log
  "The per-conversation event log — the source of truth of agentic/v1. Every turn appends events from
   the closed type set below with dense, monotonic sequence numbers; conversation state is a fold over
   the log (`reduce-state`), and a turn's normalized result is a fold over that turn's events
   (`turn-result`), so a duplicate delivery or a restarted runtime answers from the log alone.

   Event types are kebab-case keywords in Clojure (`:turn-received`) and snake_case on the wire
   (`turn_received`); `->wire` renders a result per spec/v1/result.schema.json."
  (:require [clojure.string :as str]))

(def event-types
  "The closed set of v1 event types. `append-event!` rejects anything else; a reducer ignores types it
   does not know so a log written by a newer minor version still replays."
  #{:turn-received :guardrail-rejected :routed :brain-started :tool-called :tool-failed :retrieved
    :reply-drafted :verification-failed :memory-written :turn-completed :turn-failed :turn-suspended
    :turn-resumed :timer-scheduled :timer-fired :compensation-started :compensation-step
    :compensation-completed :delegated})

(def statuses #{:completed :rejected :unverified :failed :suspended :duplicate})

(defprotocol EventLog
  (append-event! [log cid event]
    "Append {:turn-id :type :payload} to the conversation, assigning the next dense :sequence.
     Returns the stored event. Callers serialize appends per conversation (see agentic.mailbox);
     the log still refuses to store two events with one sequence.")
  (conversation-events [log cid] "All events of a conversation, ordered by :sequence.")
  (conversation-ids [log] "Every conversation the log holds."))

(defn- check-type! [event]
  (when-not (contains? event-types (:type event))
    (throw (ex-info (str "unknown event type " (:type event)) {:error/class :fatal :event event}))))

;; ---- in-memory log ----

(defrecord InMemoryEventLog [!log]
  EventLog
  (append-event! [_ cid event]
    (check-type! event)
    (let [[_ after] (swap-vals! !log update cid
                                (fn [events]
                                  (let [events (or events [])]
                                    (conj events (assoc event :sequence (count events))))))]
      (peek (get after cid))))
  (conversation-events [_ cid] (get @!log cid []))
  (conversation-ids [_] (vec (keys @!log))))

(defn in-memory-event-log [] (->InMemoryEventLog (atom {})))

;; ---- folds ----

(defn reduce-state
  "Conversation state as a fold over its events — the only definition of state."
  [events]
  (reduce (fn [state {:keys [type payload]}]
            (case type
              :turn-received (update state :turn-count inc)
              :memory-written (update state :transcript-length + (count (:messages payload)))
              :retrieved (assoc state :last-retrieved-ids (vec (:ids payload)))
              state))
          {:turn-count 0 :transcript-length 0}
          events))

(defn dense?
  "True when the sequences are exactly 0..n-1 in order."
  [events]
  (= (map :sequence events) (range (count events))))

(defn turn-events
  "The events of one turn, in order."
  [events turn-id]
  (filterv #(= turn-id (:turn-id %)) events))

(defn- last-delivery
  "A suspended turn resumes as a fresh delivery; its result covers the events from the resumption on."
  [events]
  (let [idx (last (keep-indexed (fn [i e] (when (= :turn-resumed (:type e)) i)) events))]
    (if idx (subvec events idx) events)))

(defn- tool-call [{:keys [type payload]}]
  (case type
    (:tool-called :delegated :compensation-step)
    (cond-> {:tool (:tool payload) :index (:index payload) :attempt (:attempt payload)
             :args (:args payload)}
      (contains? payload :result) (assoc :result (:result payload)))
    :tool-failed
    {:tool (:tool payload) :index (:index payload) :attempt (:attempt payload)
     :args (:args payload) :error (:error payload)}
    nil))

(defn turn-result
  "The normalized result of turn `turn-id` derived from the conversation log: status from the terminal
   event, path from `routed`, reply from `turn_completed` (or the last draft when verification gave
   up), tool calls from the tool events, and the state folded up to this turn's last event."
  [cid turn-id events]
  (let [whole (turn-events events turn-id)
        mine (last-delivery whole)
        by-type (group-by :type mine)
        terminal (last (filter #(#{:turn-completed :turn-failed :turn-suspended} (:type %)) mine))
        payload (:payload terminal)
        status (case (:type terminal)
                 :turn-completed :completed
                 :turn-suspended :suspended
                 :turn-failed (or (some-> (:status payload) keyword) :failed)
                 nil)
        error (when (= :turn-failed (:type terminal)) (:error payload))
        reply (cond
                (= :completed status) (:reply payload)
                (= :verification (:class error)) (:reply (:payload (last (:reply-drafted by-type))))
                :else nil)
        upto (if terminal
               (filterv #(<= (:sequence %) (:sequence terminal)) events)
               events)]
    {:conversation-id cid
     :turn-id turn-id
     :status status
     :path (:path (:payload (last (filter #(= :routed (:type %)) whole))))
     :reply reply
     :state (reduce-state upto)
     :tool-calls (vec (keep tool-call mine))
     :events (mapv #(select-keys % [:type :sequence :payload]) mine)
     :error error}))

(defn suspended-turns
  "turn-id → the `turn_suspended` payload of every turn suspended and not yet resumed."
  [events]
  (reduce (fn [m {:keys [type payload]}]
            (case type
              :turn-suspended (assoc m (:turn-id payload) payload)
              :turn-resumed (dissoc m (:turn-id payload))
              m))
          {} events))

(defn known-turn?
  "Has this turn id been delivered before on the conversation?"
  [events turn-id]
  (boolean (some #(= turn-id (:turn-id %)) events)))

;; ---- wire form ----

(defn- wire-key [k]
  (if (keyword? k) (str/replace (name k) "-" "_") (str k)))

(defn ->wire-value
  "Kebab keywords to snake_case strings, recursively, for payloads and state."
  [v]
  (cond (map? v) (reduce-kv (fn [m k x] (assoc m (wire-key k) (->wire-value x))) {} v)
        (sequential? v) (mapv ->wire-value v)
        (keyword? v) (wire-key v)
        :else v))

(defn ->wire
  "A normalized result in the shape of spec/v1/result.schema.json (string keys, snake_case)."
  [result]
  (let [r (select-keys result [:conversation-id :turn-id :status :path :reply :state :tool-calls :events :error])]
    (-> (->wire-value r)
        (update "tool_calls" (fn [calls] (mapv #(into {} (remove (comp nil? val)) %) calls)))
        (cond-> (contains? result :runtime-detail) (assoc "runtime_detail" (->wire-value (:runtime-detail result)))))))
