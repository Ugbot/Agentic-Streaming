(ns agentic.graph
  "The router→path→verifier turn pipeline, written as events: every step appends to the conversation
   log through the context and the caller derives the normalized result from the log afterwards
   (agentic.log/turn-result). A graph is a map:
     {:router     (fn [event ctx] -> path-key)
      :paths      {path-key {:name :prompt :brain (fn [user-text ctx] -> reply) :suspend-until
                             :verifier (fn [reply ctx] -> [ok? reply])}}   ; optional, per path
      :verifier   (fn [reply ctx] -> [ok? reply])   ; for paths without their own
      :guardrails [{:check-input (fn [text]->reason|nil) :check-output (fn [reply]->reason|nil)}]
      :policies   canonical `policies` block (retry, verification, on-tool-error, ...)
      :saga       canonical `saga` block, when the workflow runs a saga instead of a brain
      :cep        [agentic.cep-fold patterns] evaluated in-turn after `routed`
      :timers     canonical `timers` block (agentic.workflow-timers), scheduled on the first turn
      :listeners  [..]}

   Legacy transcript/attribute writes to the ConversationStore are kept as a projection of the log,
   so the Datomic time-travel and CEP paths keep reading them."
  (:require [clojure.string :as str]
            [agentic.store :as store]
            [agentic.context :as ctx]
            [agentic.tools :as tools]
            [agentic.cep-fold :as cep-fold]
            [agentic.listener :as listener]
            [agentic.workflow-timers :as wt]))

(def phase-attr "phase")
(def path-attr "path")

(defn- attr! [c k v]
  (when-let [s (:store c)] (store/put-attribute s (:conversation-id c) k v)))

(defn- verification-policy [graph]
  (merge {:max-attempts 1 :on-exhausted "unverified"} (get-in graph [:policies :verification])))

(defn- guardrail-reason [graph stage text]
  (some (fn [g] (when-let [f (get g stage)] (f text))) (:guardrails graph)))

(defn- finish-rejected [graph c reason]
  (listener/fire (:listeners c) :on-guardrail-block {:reason reason :ctx c})
  (ctx/emit! c :guardrail-rejected {:reason reason})
  (ctx/emit! c :turn-failed {:status "rejected" :error {:class :guardrail :message reason}})
  (attr! c phase-attr "done")
  :rejected)

(defn- finish-tool-failure [c e]
  (ctx/emit! c :turn-failed {:status "failed" :reason (ex-message e)
                             :error {:class :tool :message (ex-message e)}})
  (attr! c phase-attr "done")
  :failed)

(defn- validation-error? [e] (= :validation (:error/class (ex-data e))))

(defn- finish-validation-failure [c e]
  (ctx/emit! c :turn-failed {:status "failed" :reason (ex-message e)
                             :error {:class :validation :message (ex-message e)}})
  (attr! c phase-attr "done")
  :failed)

(defn- complete! [graph c path reply]
  (when-let [s (:store c)]
    (store/associate-user s (:conversation-id c) (:user-id c))
    (store/append s (:conversation-id c) {:role "user" :content (:text c)})
    (store/append s (:conversation-id c) {:role "assistant" :content reply}))
  (ctx/emit! c :memory-written {:messages [{:role "user" :text (:text c)}
                                           {:role "assistant" :text reply}]})
  (ctx/emit! c :turn-completed {:reply reply})
  (attr! c phase-attr "done")
  (listener/fire (:listeners c) :on-turn-end {:path path :reply reply :ctx c})
  :completed)

(defn verifier-for
  "The verifier that judges turns routed to `path`: the path's own when it declares one, else the
   graph-level one (`agent.verifier`); nil when neither verifies."
  [graph path]
  (or (get-in graph [:paths path :verifier]) (:verifier graph)))

(defn- verify [graph path reply c]
  (if-let [v (verifier-for graph path)]
    (let [[ok? vreply] (v reply c)] [(boolean ok?) vreply])
    [true reply]))

(defn run-brain
  "brain_started → up to `verification.max_attempts` drafts, each verified; the first accepted draft
   completes the turn, a tool error fails it, exhaustion ends `unverified` or `failed` per
   `on_exhausted`. Returns the terminal status keyword."
  [graph c path]
  (let [agent (get-in graph [:paths path])
        {:keys [max-attempts on-exhausted]} (verification-policy graph)]
    (ctx/emit! c :brain-started {:path path})
    (attr! c phase-attr (str "path:" path))
    (loop [attempt 1]
      (let [outcome (try {:reply ((:brain agent) (:text c) c)}
                         (catch clojure.lang.ExceptionInfo e
                           (cond (ctx/tool-error? e) {:tool-error e}
                                 (validation-error? e) {:validation-error e}
                                 :else (throw e))))]
        (cond
          (:tool-error outcome) (finish-tool-failure c (:tool-error outcome))
          (:validation-error outcome) (finish-validation-failure c (:validation-error outcome))
          :else
          (let [reply (:reply outcome)]
            (ctx/emit! c :reply-drafted {:reply reply})
            (if-let [reason (guardrail-reason graph :check-output reply)]
              (finish-rejected graph c reason)
              (let [[ok? vreply] (verify graph path reply c)]
                (cond
                  ok? (complete! graph c path vreply)

                  :else
                  (do (ctx/emit! c :verification-failed {:reply reply})
                      (if (< attempt (max 1 (int max-attempts)))
                        (recur (inc attempt))
                        (let [status (if (= "unverified" on-exhausted) "unverified" "failed")]
                          (ctx/emit! c :turn-failed {:status status
                                                     :error {:class :verification
                                                             :message "verifier rejected the reply"}})
                          (attr! c phase-attr "done")
                          (keyword status)))))))))))))

(defn- step-name [step] (or (:name step) (:tool step)))

(defn run-saga
  "Execute the saga's steps in declaration order through `call-tool`; when one fails, compensate every
   completed step in reverse order (its `compensate_with`, else the tool's `compensation`) and fail
   the turn. Returns the terminal status keyword."
  [graph c path]
  (let [steps (get-in graph [:saga :steps])]
    (attr! c phase-attr (str "saga:" path))
    (loop [[step & more] steps done []]
      (if (nil? step)
        (complete! graph c path (str "[" path "] saga completed"))
        (let [outcome (try {:ok (ctx/call-tool c (:tool step) (:args step))}
                           (catch clojure.lang.ExceptionInfo e
                             (if (ctx/tool-error? e) {:tool-error e} (throw e))))]
          (if-let [e (:tool-error outcome)]
            (do (ctx/emit! c :compensation-started {:failed-step (step-name step)})
                (doseq [completed (reverse done)]
                  (when-let [undo (or (:compensate-with completed)
                                      (tools/compensation (:tools c) (:tool completed)))]
                    (ctx/call-tool c undo {} :compensation-step)))
                (ctx/emit! c :compensation-completed {:steps (count done)})
                (finish-tool-failure c e))
            (recur more (conj done step))))))))

(defn received-payload
  "The `turn_received` payload: turn id and text, plus the turn's metadata and its event time
   (`metadata.event_time_ms` as a number) when it carries any."
  [event]
  (let [metadata (:metadata event)
        event-time (cep-fold/event-time-ms metadata)]
    (cond-> {:turn-id (:turn-id event) :text (:text event)}
      (seq metadata) (assoc :metadata metadata)
      (some? event-time) (assoc :event-time-ms event-time))))

(defn- match-patterns
  "Sequence CEP over the conversation's log, this turn included; a pattern completing on this turn
   invokes its tool on this turn. Returns :failed when that tool failed, else nil."
  [graph c]
  (when (seq (:cep graph))
    (let [outcome (try (cep-fold/evaluate! (:cep graph) c ((:events c) (:conversation-id c))) nil
                       (catch clojure.lang.ExceptionInfo e
                         (if (ctx/tool-error? e) {:tool-error e} (throw e))))]
      (when-let [e (:tool-error outcome)]
        (finish-tool-failure c e)))))

(defn handle
  "Process one fresh turn: turn_received → guardrails → routed → cep → suspend, saga or brain. Returns
   the terminal status keyword; the events are in the log."
  [graph event c]
  (let [listeners (:listeners c) text (:text event)]
    (listener/fire listeners :on-turn-start {:event event :ctx c})
    (wt/fire-due! graph event c)
    (ctx/emit! c :turn-received
               (wt/received-payload graph event c (received-payload (assoc event :turn-id (:turn-id c)))))
    (wt/schedule! graph event c)
    (if-let [reason (guardrail-reason graph :check-input text)]
      (finish-rejected graph c reason)
      (do
        (attr! c phase-attr "routing")
        (let [path ((:router graph) event c)
              agent (get-in graph [:paths path])]
          (when (nil? agent)
            (throw (ex-info (str "router chose unknown path " path) {:error/class :fatal :path path})))
          (ctx/emit! c :routed {:path path})
          (attr! c path-attr path)
          (listener/fire listeners :on-routed {:path path :ctx c})
          (cond
            (= :failed (match-patterns graph c)) :failed

            (contains? agent :suspend-until)
            (do (ctx/emit! c :turn-suspended {:turn-id (:turn-id c) :path path :text text
                                              :until (:suspend-until agent)})
                (attr! c phase-attr "suspended")
                :suspended)

            (:saga graph) (run-saga graph c path)
            :else (run-brain graph c path)))))))

(defn resume
  "Continue a suspended turn after its signal arrived: turn_resumed, then the path's brain over the
   originally received text."
  [graph c pending signal]
  (ctx/emit! c :turn-resumed {:turn-id (:turn-id c) :signal signal})
  (run-brain graph c (:path pending)))

(defn prefix-verifier
  "The default v1 verifier: a reply is accepted when it starts with `[`."
  [reply _ctx]
  [(boolean (and reply (str/starts-with? reply "["))) reply])
