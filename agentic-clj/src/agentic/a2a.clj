(ns agentic.a2a
  "A2A (Agent-to-Agent) client — call a peer agent's HTTP gateway as a tool, with Agent Card
   discovery + bounded retries/backoff. `peer-tool` turns a peer into a `ToolRegistry` tool so a path
   can delegate a turn to another agent (on any backend). The Clojure mirror of jagentic-core's
   A2AClient and pyagentic's a2a.py."
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.string :as str]))

(defrecord A2AClient [base-url retries backoff timeout])

(defn a2a-client
  "Build an A2A client against `base-url` (trailing slashes stripped). Options: :retries (default 2),
   :backoff ms (default 200), :timeout ms (default 30000)."
  [base-url & {:keys [retries backoff timeout] :or {retries 2 backoff 200 timeout 30000}}]
  (->A2AClient (str/replace (str base-url) #"/+$" "") (max 0 (int retries)) (int backoff) (int timeout)))

(defn card
  "GET <base>/.well-known/agent-card.json -> parsed map."
  [client]
  (let [resp (http/get (str (:base-url client) "/.well-known/agent-card.json")
                       {:socket-timeout (:timeout client) :connection-timeout (:timeout client)})]
    (json/read-str (:body resp))))

(defn send-turn
  "POST <base>/agent with {conversation_id, text, user_id}. Retries on exception with exponential
   backoff (backoff * 2^attempt); throws after exhausting retries. Returns the parsed response map."
  [client {:keys [conversation-id text user-id] :or {conversation-id "a2a" text "" user-id "anonymous"}}]
  (let [url (str (:base-url client) "/agent")
        body (json/write-str {"conversation_id" conversation-id "text" text "user_id" user-id})
        retries (:retries client)]
    (loop [attempt 0]
      (let [result (try
                     {:ok (json/read-str
                           (:body (http/post url {:body body :content-type :json
                                                  :socket-timeout (:timeout client)
                                                  :connection-timeout (:timeout client)})))}
                     (catch Exception e {:err e}))]
        (if (contains? result :ok)
          (:ok result)
          (if (< attempt retries)
            (do (Thread/sleep (* (:backoff client) (bit-shift-left 1 attempt)))
                (recur (inc attempt)))
            (throw (ex-info (str "A2A send to " url " failed after " (inc retries) " tries: "
                                 (.getMessage ^Exception (:err result)))
                            {:url url} (:err result)))))))))

(defn peer-tool
  "A ToolRegistry tool fn that delegates a turn to a peer agent. Reads conversation_id (default
   \"a2a\"), text (default \"\"), user_id (default \"anonymous\") from params; returns the peer reply
   map. `retries` bounds the send."
  [base-url retries]
  (let [client (a2a-client base-url :retries (or retries 2))]
    (fn [params]
      (send-turn client {:conversation-id (str (get params "conversation_id" "a2a"))
                         :text (str (get params "text" ""))
                         :user-id (str (get params "user_id" "anonymous"))}))))
