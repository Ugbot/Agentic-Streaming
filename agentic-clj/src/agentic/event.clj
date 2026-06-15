(ns agentic.event
  "An inbound message. Just a map — {:conversation-id :user-id :text :metadata}.")

(defn event
  ([cid uid text] (event cid uid text {}))
  ([cid uid text metadata]
   {:conversation-id cid :user-id uid :text text :metadata (or metadata {})}))
