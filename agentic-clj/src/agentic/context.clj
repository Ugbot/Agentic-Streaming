(ns agentic.context
  "The per-turn context passed to brains: stores + tools + retriever + listeners, plus call-tool
   (which records the tool id and fires listener hooks). Mirror of jagentic-core AgentContext."
  (:require [agentic.tools :as tools]
            [agentic.listener :as listener]))

(defn make-context [{:keys [conversation-id user-id store state tools retriever listeners]}]
  {:conversation-id conversation-id
   :user-id user-id
   :store store
   :state state
   :tools tools
   :retriever retriever
   :listeners (or listeners [])
   :tool-calls (atom [])})

(defn call-tool [ctx tool-id params]
  (swap! (:tool-calls ctx) conj tool-id)
  (listener/fire (:listeners ctx) :on-tool-call-start {:tool tool-id :ctx ctx})
  (let [result (tools/execute (:tools ctx) tool-id params)]
    (listener/fire (:listeners ctx) :on-tool-call-end {:tool tool-id :result result :ctx ctx})
    result))
