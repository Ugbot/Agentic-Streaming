(ns agentic.context
  "The per-turn context brains run in: the stores, tools and retriever plus `emit!`, the single door
   to the conversation's event log for this turn. `call-tool` is the only way a brain invokes a tool —
   it hands out dense tool-call indexes, retries per `policies.retry` with one attempt-numbered
   `tool_called`/`tool_failed` event per attempt, and applies `policies.on_tool_error`. `retrieve`
   records a `retrieved` event with the ordered hit ids."
  (:require [agentic.tools :as tools]
            [agentic.retrieval :as r]
            [agentic.listener :as listener]))

(defn make-context
  [{:keys [conversation-id user-id turn-id text store state tools retriever listeners emit! policies]}]
  {:conversation-id conversation-id
   :user-id user-id
   :turn-id turn-id
   :text text
   :store store
   :state state
   :tools tools
   :retriever retriever
   :listeners (or listeners [])
   :policies (or policies {})
   :emit! (or emit! (fn [_type _payload] nil))
   :tool-calls (atom [])
   :tool-index (atom -1)})

(defn emit!
  "Append an event of `type` with `payload` to this turn's conversation log; returns the stored event."
  [ctx type payload]
  ((:emit! ctx) type payload))

(defn tool-error
  "The error a failed tool surfaces to the turn: class :tool, no runtime detail."
  [tool-id message cause]
  (ex-info message {:error/class :tool :tool tool-id} cause))

(defn tool-error? [t] (= :tool (:error/class (ex-data t))))

(defn- max-attempts [policies]
  (let [{:keys [kind max-attempts] :or {kind "none" max-attempts 1}} (:retry policies)]
    (if (= "none" kind) 1 (max 1 (int max-attempts)))))

(defn- retry-delay-ms [policies attempt]
  (let [{:keys [kind initial-delay-ms max-delay-ms multiplier]
         :or {kind "none" initial-delay-ms 0 max-delay-ms Long/MAX_VALUE multiplier 2.0}} (:retry policies)]
    (case kind
      "fixed" (long initial-delay-ms)
      "exponential" (long (min max-delay-ms (* initial-delay-ms (Math/pow multiplier (dec attempt)))))
      0)))

(defn call-tool
  "Invoke `tool-id` with structured `args` under the turn's retry policy. Returns the tool's result,
   nil when the last attempt failed and `on_tool_error` is `continue`, else throws a `tool-error`.
   `event-type` overrides the success event type (a saga's `compensation_step`)."
  ([ctx tool-id args] (call-tool ctx tool-id args nil))
  ([ctx tool-id args event-type]
   (let [index (swap! (:tool-index ctx) inc)
         attempts (max-attempts (:policies ctx))
         success-type (or event-type
                          (if (= "agent" (tools/kind (:tools ctx) tool-id)) :delegated :tool-called))
         args (or args {})]
     (listener/fire (:listeners ctx) :on-tool-call-start {:tool tool-id :ctx ctx})
     (loop [attempt 1]
       (let [outcome (try {:result (tools/execute (:tools ctx) tool-id args)}
                          (catch Exception e {:error e}))]
         (if (contains? outcome :result)
           (let [result (:result outcome)]
             (swap! (:tool-calls ctx) conj {:tool tool-id :index index :attempt attempt :args args :result result})
             (emit! ctx success-type {:tool tool-id :index index :attempt attempt :args args :result result})
             (listener/fire (:listeners ctx) :on-tool-call-end {:tool tool-id :result result :ctx ctx})
             result)
           (let [^Exception e (:error outcome)
                 message (or (ex-message e) (str tool-id " failed"))]
             (swap! (:tool-calls ctx) conj {:tool tool-id :index index :attempt attempt :args args :error message})
             (emit! ctx :tool-failed {:tool tool-id :index index :attempt attempt :args args :error message})
             (listener/fire (:listeners ctx) :on-error {:tool tool-id :error e :ctx ctx})
             (cond
               (< attempt attempts)
               (do (let [ms (retry-delay-ms (:policies ctx) attempt)]
                     (when (pos? ms) (Thread/sleep ms)))
                   (recur (inc attempt)))

               (= "continue" (:on-tool-error (:policies ctx) "fail"))
               nil

               :else
               (throw (tool-error tool-id message e))))))))))

(defn retrieve
  "Top-k hits for `query` from the turn's retriever, recording a `retrieved` event with their ids.
   Returns [] without an event when the system has no retriever."
  [ctx query k]
  (if-let [retriever (:retriever ctx)]
    (let [hits (r/retrieve retriever query k)]
      (emit! ctx :retrieved {:ids (mapv :id hits)})
      hits)
    []))
