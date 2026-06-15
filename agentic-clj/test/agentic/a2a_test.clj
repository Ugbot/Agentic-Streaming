(ns agentic.a2a-test
  (:require [clojure.test :refer [deftest is testing]]
            [org.httpkit.server :as hk]
            [clojure.data.json :as json]
            [agentic.a2a :as a2a]
            [agentic.tools :as tools]))

(defn- handler [req]
  (let [uri (:uri req) method (:request-method req)]
    (cond
      (and (= method :get) (= uri "/.well-known/agent-card.json"))
      {:status 200 :headers {"Content-Type" "application/json"}
       :body (json/write-str {"name" "peer"})}

      (and (= method :post) (= uri "/agent"))
      (let [in (json/read-str (slurp (:body req)))]
        {:status 200 :headers {"Content-Type" "application/json"}
         :body (json/write-str {"conversation_id" (get in "conversation_id")
                                "reply" "pong" "ok" true})})

      :else {:status 404 :body "{}"})))

(deftest a2a-client-card-send-and-peer-tool
  (let [stop (hk/run-server handler {:ip "127.0.0.1" :port 0})
        port (:local-port (meta stop))
        base (str "http://127.0.0.1:" port)]
    (try
      (let [client (a2a/a2a-client base)]
        (testing "card returns the agent card map"
          (is (= "peer" (get (a2a/card client) "name"))))
        (testing "send-turn posts and parses the reply"
          (let [resp (a2a/send-turn client {:conversation-id "c1" :text "hi" :user-id "u"})]
            (is (= "pong" (get resp "reply")))
            (is (true? (get resp "ok")))
            (is (= "c1" (get resp "conversation_id")))))
        (testing "peer-tool registered + executed delegates the turn"
          (let [reg (tools/registry)]
            (tools/register reg "peer" "delegate to peer" (a2a/peer-tool base 2))
            (let [out (tools/execute reg "peer" {"conversation_id" "c2" "text" "yo" "user_id" "x"})]
              (is (= "pong" (get out "reply")))))))
      (finally (stop)))))
