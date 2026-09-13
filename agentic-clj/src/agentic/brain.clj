(ns agentic.brain
  "Generic, declarative brains the pipeline loader wires from a spec. keyword-brain mirrors the
   jagentic-core/pyagentic pipeline KeywordBrain: fire a tool on a trigger keyword, else answer from
   retrieval, else echo."
  (:require [clojure.string :as str]
            [agentic.context :as ctx]
            [agentic.retrieval :as r]))

(defn keyword-brain
  "The v1 `rule` brain: the first trigger keyword found in the text fires its tool with
   `{\"user\" user-id}`; else the top-k retrieval hits are recorded and the best one answers when it
   clears the threshold; else an echo. Every reply carries the `[path]` prefix the verifier expects."
  [name {:keys [tool-triggers threshold dim top-k] :or {threshold 0.15 dim 256 top-k 4}}]
  (fn [user-text context]
    (let [low (str/lower-case user-text)]
      (or
       (some (fn [[kw tool]]
               (when (str/includes? low (str/lower-case kw))
                 (let [result (ctx/call-tool context tool {"user" (:user-id context)})]
                   (str "[" name "] " tool " returned " result))))
             tool-triggers)
       (when (:retriever context)
         (let [hits (ctx/retrieve context (r/embed user-text dim) top-k)]
           (when (and (seq hits) (> (:score (first hits)) threshold))
             (str "[" name "] " (:text (first hits))))))
       (str "[" name "] I can help with " name " questions. You said: \"" user-text "\"")))))
