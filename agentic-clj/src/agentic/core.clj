(ns agentic.core
  "The local runtime seam: build a system (graph + tools + retriever + stores + event log) and submit
   turns. Each conversation has one mailbox (a Clojure agent): deliveries are enqueued in arrival
   order and processed one at a time by a single writer. `submit` is also the idempotency point: a turn id
   seen before answers from the log with status `duplicate` and no new events; a signal for a
   suspended turn resumes it. The normalized result is derived from the log, never assembled."
  (:require [agentic.graph :as graph]
            [agentic.context :as ctx]
            [agentic.store :as store]
            [agentic.log :as log]
            [agentic.banking :as banking])
  (:import [java.util UUID]))

(defn local-system
  "A runnable system over the given graph/tools/retriever. `opts` may carry :store, :state and :log
   (defaults are in-memory); pass a system's :log to a new system to restart over the same history."
  ([graph tools retriever]
   (local-system graph tools retriever {}))
  ([graph tools retriever conversation-store keyed-state-store]
   (local-system graph tools retriever {:store conversation-store :state keyed-state-store}))
  ([graph tools retriever {:keys [store state log]}]
   {:graph graph :tools tools :retriever retriever
    :store (or store (store/in-memory-conversation-store))
    :state (or state (store/in-memory-keyed-state-store))
    :log (or log (log/in-memory-event-log))
    :mailboxes (atom {})}))

(defn restart
  "The same workflow over the same log and stores, with every derived structure dropped."
  [system]
  (assoc system :mailboxes (atom {})))

(defn- mailbox
  "The conversation's single-writer mailbox; an agent's actions run serially in send order."
  [system cid]
  (get (swap! (:mailboxes system) update cid #(or % (agent nil :error-mode :continue))) cid))

(defn- turn-id-of [event]
  (or (:turn-id event) (get-in event [:metadata "turn_id"]) (str (UUID/randomUUID))))

(defn- make-ctx [system event turn-id text]
  (let [cid (:conversation-id event)
        elog (:log system)]
    (ctx/make-context
     {:conversation-id cid
      :user-id (or (:user-id event) "anonymous")
      :turn-id turn-id
      :text text
      :store (:store system)
      :state (:state system)
      :tools (:tools system)
      :retriever (:retriever system)
      :listeners (get-in system [:graph :listeners])
      :policies (get-in system [:graph :policies])
      :emit! (fn [type payload]
               (log/append-event! elog cid {:turn-id turn-id :type type :payload payload}))
      :events (fn [conversation-id] (log/conversation-events elog conversation-id))})))

(defn- run-guarded
  "Run `f`; anything that is not a turn-level outcome is recorded as a fatal `turn_failed` so the log
   still carries a terminal event, then rethrown."
  [c f]
  (try (f)
       (catch Exception e
         (ctx/emit! c :turn-failed {:status "failed" :error {:class :fatal :message (ex-message e)}})
         (throw e))))

(defn- result [system cid turn-id]
  (let [r (log/turn-result cid turn-id (log/conversation-events (:log system) cid)
                           (get-in system [:graph :context]))]
    (assoc r :ok (= :completed (:status r)))))

(defn- process
  "Run one delivery on the conversation's writer thread."
  [system event turn-id]
  (let [cid (:conversation-id event)
        idempotent? (not= "none" (get-in system [:graph :policies :idempotency] "turn-id"))
        events (log/conversation-events (:log system) cid)
        pending (get (log/suspended-turns events) turn-id)]
    (cond
      (and (:signal event) pending)
      (let [c (make-ctx system event turn-id (:text pending))]
        (run-guarded c #(graph/resume (:graph system) c pending (:signal event)))
        (result system cid turn-id))

      (and idempotent? (log/known-turn? events turn-id))
      (-> (result system cid turn-id)
          (assoc :status :duplicate :ok false :events []))

      :else
      (let [c (make-ctx system event turn-id (:text event))]
        (run-guarded c #(graph/handle (:graph system) (assoc event :turn-id turn-id) c))
        (result system cid turn-id)))))

(defn submit-async
  "Enqueue one turn on its conversation's mailbox and return a promise of its normalized result.
   The enqueue is the arrival: two calls made in order are processed in that order, however the
   callers are scheduled. A failure is delivered as the exception itself."
  [system event]
  (let [turn-id (turn-id-of event)
        outcome (promise)]
    (send-off (mailbox system (:conversation-id event))
              (fn [_]
                (deliver outcome (try (process system event turn-id)
                                      (catch Throwable t t)))
                nil))
    outcome))

(defn submit
  "Deliver one turn `{:conversation-id :turn-id :user-id :text :signal}` and return its normalized
   result (agentic.log/turn-result plus :ok). Without a :turn-id a fresh one is minted, which makes
   the delivery non-idempotent by construction."
  [system event]
  (let [r @(submit-async system event)]
    (if (instance? Throwable r) (throw r) r)))

(defn events
  "The conversation's event log."
  [system cid]
  (log/conversation-events (:log system) cid))

(defn state
  "Conversation state, folded from the log under the workflow's `context` block."
  [system cid]
  (log/reduce-state (events system cid) (get-in system [:graph :context])))

(defn banking-system []
  (local-system (banking/build-graph) (banking/default-tools) (banking/retriever)))
