(ns agentic.http-test
  (:require [clojure.test :refer [deftest is testing]]
            [clj-http.client :as http]
            [clojure.data.json :as json]
            [agentic.http :as ahttp]
            [agentic.core :as core]))

(deftest agent-card-and-agent-endpoint
  (let [sys (core/banking-system)
        stop (ahttp/start sys {:host "127.0.0.1" :port 0})
        port (:local-port (meta stop))
        base (str "http://127.0.0.1:" port)]
    (try
      (testing "agent card"
        (let [resp (http/get (str base "/.well-known/agent-card.json"))
              body (json/read-str (:body resp) :key-fn keyword)]
          (is (= 200 (:status resp)))
          (is (= "agentic-clj" (:name body)))))
      (testing "POST /agent"
        (let [resp (http/post (str base "/agent")
                              {:body "{\"conversation_id\":\"c1\",\"user_id\":\"u\",\"text\":\"what is my balance?\"}"
                               :content-type :json})
              body (json/read-str (:body resp) :key-fn keyword)]
          (is (= 200 (:status resp)))
          (is (= "payments" (:path body)))
          (is (re-find #"1234\.56" (:reply body)))))
      (finally (stop)))))
