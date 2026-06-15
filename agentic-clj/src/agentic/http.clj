(ns agentic.http
  "HTTP front door (http-kit): the Agent Card + POST /agent — A2A-interoperable, mirroring the other
   gateways. Turns run through the runtime's submit."
  (:require [org.httpkit.server :as hk]
            [clojure.data.json :as json]
            [agentic.core :as core]
            [agentic.banking :as banking]
            [agentic.event :as ev]))

(defn agent-card [base-url]
  {:name "agentic-clj"
   :description "Agentic Clojure — a pure-Clojure agent runtime on Datomic (router→path→verifier)."
   :url base-url
   :version "0.1.0"
   :capabilities {:streaming false}
   :skills [{:id "chat" :name "Conversational agent"
             :description "Processes a turn for a conversation; transcript durable in Datomic."}]})

(defn handler [system base-url]
  (fn [req]
    (let [uri (:uri req) method (:request-method req)]
      (cond
        (and (= method :get) (= uri "/.well-known/agent-card.json"))
        {:status 200 :headers {"Content-Type" "application/json"}
         :body (json/write-str (agent-card base-url))}

        (and (= method :get) (= uri "/healthz"))
        {:status 200 :headers {"Content-Type" "application/json"} :body "{\"status\":\"ok\"}"}

        (and (= method :post) (= uri "/agent"))
        (let [in (json/read-str (slurp (:body req)) :key-fn keyword)
              e (ev/event (or (:conversation_id in) (:conversationId in) "c")
                          (or (:user_id in) (:userId in) "anonymous")
                          (or (:text in) ""))
              r (core/submit system e)]
          {:status 200 :headers {"Content-Type" "application/json"}
           :body (json/write-str {:conversation_id (:conversation-id r) :reply (:reply r)
                                  :path (:path r) :ok (:ok r) :tool_calls (:tool-calls r)})})

        :else {:status 404 :headers {"Content-Type" "application/json"} :body "{\"error\":\"not found\"}"}))))

(defn start
  "Start the front door. Returns the http-kit stop-fn (its meta carries :local-port)."
  [system {:keys [host port base-url] :or {host "0.0.0.0" port 8080}}]
  (hk/run-server (handler system (or base-url (str "http://" host ":" port)))
                 {:ip host :port port}))

(defn -main [& _]
  (let [sys (core/banking-system)]
    (start sys {:host "0.0.0.0" :port 8080})
    (println "agentic-clj HTTP front door on :8080")
    @(promise)))
