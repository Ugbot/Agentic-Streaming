(ns agentic.datomic-test
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.store :as store]
            [agentic.store.datomic :as dat]
            [agentic.core :as core]
            [agentic.event :as ev]
            [agentic.banking :as banking]))

(defn- try-stores []
  (try (dat/datomic-stores {:db-name (str "test-" (System/nanoTime)) :storage-dir :mem})
       (catch Throwable t
         (println "datomic-local unavailable, skipping:" (.getMessage t))
         nil)))

(deftest datomic-conversation-store-roundtrip
  (when-let [{:keys [conversation keyed long-term]} (try-stores)]
    (testing "transcript append/history/count"
      (store/append conversation "c1" {:role "user" :content "hello"})
      (store/append conversation "c1" {:role "assistant" :content "hi there"})
      (is (= 2 (store/message-count conversation "c1")))
      (is (= [{:role "user" :content "hello"} {:role "assistant" :content "hi there"}]
             (store/history conversation "c1"))))
    (testing "attributes upsert"
      (store/put-attribute conversation "c1" "path" "payments")
      (store/put-attribute conversation "c1" "path" "cards") ; upsert
      (is (= "cards" (store/get-attribute conversation "c1" "path")))
      (is (= {"path" "cards"} (store/attributes conversation "c1"))))
    (testing "user index"
      (store/associate-user conversation "c1" "alice")
      (is (= ["c1"] (store/conversations-for-user conversation "alice"))))
    (testing "keyed state"
      (store/kv-put keyed "c1" "tier" "gold")
      (is (= "gold" (store/kv-get keyed "c1" "tier"))))
    (testing "long-term facts + resume"
      (store/save-turn long-term "c1" "alice" "user" "hi")
      (store/save-turn long-term "c1" "alice" "assistant" "hello")
      (is (= [["user" "hi"] ["assistant" "hello"]] (store/load-history long-term "c1")))
      (store/save-fact long-term "alice" "tier" "gold")
      (is (= {"tier" "gold"} (store/facts long-term "alice"))))))

(deftest banking-runs-on-datomic-stores
  (when-let [{:keys [conversation keyed]} (try-stores)]
    (let [sys (core/local-system (banking/build-graph) (banking/default-tools) (banking/retriever)
                                 conversation keyed)
          r (core/submit sys (ev/event "c1" "alice" "what is my balance?"))]
      (is (= "payments" (:path r)))
      (is (= "[payments] Your balance is 1234.56." (:reply r)))
      ;; transcript durably in Datomic
      (is (= 2 (store/message-count conversation "c1"))))))
