(ns agentic.core
  "The local runtime seam: build a system (graph + tools + retriever + stores) and submit events.
   Single-writer per conversation is the store's concern (in-memory is process-local; Datomic is the
   durable backend)."
  (:require [agentic.graph :as graph]
            [agentic.context :as ctx]
            [agentic.store :as store]
            [agentic.banking :as banking]))

(defn local-system
  "A runnable system over the given graph/tools/retriever, with in-memory stores by default."
  ([graph tools retriever]
   (local-system graph tools retriever
                 (store/in-memory-conversation-store) (store/in-memory-keyed-state-store)))
  ([graph tools retriever conversation-store keyed-state-store]
   {:graph graph :tools tools :retriever retriever
    :store conversation-store :state keyed-state-store}))

(defn submit
  "Process one turn for an event, returning the turn-result map."
  [system event]
  (let [context (ctx/make-context
                 {:conversation-id (:conversation-id event)
                  :user-id (:user-id event)
                  :store (:store system)
                  :state (:state system)
                  :tools (:tools system)
                  :retriever (:retriever system)
                  :listeners (get-in system [:graph :listeners])})]
    (graph/handle (:graph system) event context)))

(defn banking-system []
  (local-system (banking/build-graph) (banking/default-tools) (banking/retriever)))
