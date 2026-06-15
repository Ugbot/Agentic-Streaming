(ns agentic.brain
  "Generic, declarative brains the pipeline loader wires from a spec. keyword-brain mirrors the
   jagentic-core/pyagentic pipeline KeywordBrain: fire a tool on a trigger keyword, else answer from
   retrieval, else echo."
  (:require [clojure.string :as str]
            [agentic.context :as ctx]
            [agentic.retrieval :as r]))

(defn keyword-brain
  [name {:keys [tool-triggers threshold dim] :or {threshold 0.15 dim 256}}]
  (fn [user-text context]
    (let [low (str/lower-case user-text)]
      (or
       (some (fn [[kw tool]]
               (when (str/includes? low (str/lower-case kw))
                 (let [result (ctx/call-tool context tool {"user" (:user-id context)})]
                   (str "[" name "] " tool " returned " result))))
             tool-triggers)
       (when (:retriever context)
         (let [hits (r/retrieve (:retriever context) (r/embed user-text dim) 1)]
           (when (and (seq hits) (> (:score (first hits)) threshold))
             (str "[" name "] " (:text (first hits))))))
       (str "[" name "] I can help with " name " questions. You said: \"" user-text "\"")))))
