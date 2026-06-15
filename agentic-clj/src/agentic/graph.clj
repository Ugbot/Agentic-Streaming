(ns agentic.graph
  "The router→path→verifier turn pipeline — a pure fn reproducing jagentic-core's RoutedGraph.handle:
   input guardrails → route → path brain (append user, brain, append assistant) → output guardrails
   → verifier, writing phase/path attributes and firing listener hooks. A graph is a map:
     {:router (fn [event ctx] -> path-key)
      :paths  {path-key {:name :prompt :brain (fn [user-text ctx] -> reply)}}
      :verifier (fn [reply ctx] -> [ok? reply])
      :guardrails [{:check-input (fn [text]->reason|nil) :check-output (fn [reply]->reason|nil)}]
      :listeners [..]}."
  (:require [agentic.store :as store]
            [agentic.listener :as listener]))

(def phase-attr "phase")
(def path-attr "path")

(defn- agent-turn [agent event ctx]
  (let [s (:store ctx) cid (:conversation-id ctx)]
    (store/associate-user s cid (:user-id ctx))
    (store/append s cid {:role "user" :content (:text event)})
    (let [reply ((:brain agent) (:text event) ctx)]
      (store/append s cid {:role "assistant" :content reply})
      reply)))

(defn- blocked [cid reason listeners ctx]
  (listener/fire listeners :on-guardrail-block {:reason reason :ctx ctx})
  {:conversation-id cid :reply (str "[blocked] " reason) :path "blocked" :ok false :tool-calls []})

(defn handle
  "Process one turn; returns a turn-result map {:conversation-id :reply :path :ok :tool-calls}."
  [graph event ctx]
  (let [s (:store ctx) cid (:conversation-id ctx) listeners (:listeners ctx)]
    (listener/fire listeners :on-turn-start {:event event :ctx ctx})
    (if-let [reason (some (fn [g] (when-let [f (:check-input g)] (f (:text event)))) (:guardrails graph))]
      (blocked cid reason listeners ctx)
      (do
        (store/put-attribute s cid phase-attr "routing")
        (let [path-key ((:router graph) event ctx)
              agent (get-in graph [:paths path-key])]
          (store/put-attribute s cid path-attr path-key)
          (store/put-attribute s cid phase-attr (str "path:" path-key))
          (listener/fire listeners :on-routed {:path path-key :ctx ctx})
          (let [reply (agent-turn agent event ctx)
                out-reason (some (fn [g] (when-let [f (:check-output g)] (f reply))) (:guardrails graph))]
            (if out-reason
              (blocked cid out-reason listeners ctx)
              (let [[ok? vreply] (if-let [v (:verifier graph)] (v reply ctx) [true reply])
                    result {:conversation-id cid :reply vreply :path path-key :ok (boolean ok?)
                            :tool-calls @(:tool-calls ctx)}]
                (store/put-attribute s cid phase-attr "done")
                (listener/fire listeners :on-turn-end {:result result :ctx ctx})
                result))))))))
