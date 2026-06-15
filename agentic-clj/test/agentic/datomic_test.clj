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

(deftest datomic-transcript-time-travel
  ;; the transcript is immutable datoms — history-as-of replays an earlier basis-t exactly.
  (when-let [{:keys [conn conversation]} (try-stores)]
    (store/append conversation "c1" {:role "user" :content "first"})
    (let [t1 (dat/basis-t conn)]
      (store/append conversation "c1" {:role "assistant" :content "second"})
      (store/append conversation "c1" {:role "user" :content "third"})
      (testing "current view has all three"
        (is (= 3 (count (store/history conversation "c1")))))
      (testing "as-of t1 is the strict one-message prefix"
        (let [back (dat/history-as-of conn "c1" t1)]
          (is (= 1 (count back)))
          (is (= [{:role "user" :content "first"}] back)))))))

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

;; --- client-config: the deployment selector is a pure fn, testable without any connection ---

(deftest client-config-builds-each-deployment
  (testing "in-process datomic-local is the default"
    (is (= {:server-type :datomic-local :system "agentic" :storage-dir :mem}
           (dat/client-config {})))
    (is (= :mem (:storage-dir (dat/client-config {:storage-dir "mem"}))))   ; "mem" string -> :mem
    (is (= "/var/agentic" (:storage-dir (dat/client-config {:storage-dir "/var/agentic"})))))
  (testing "Datomic Pro peer-server forwards endpoint/access-key/secret verbatim"
    (let [cfg (dat/client-config {:server-type "peer-server" :endpoint "localhost:8998"
                                  :access-key "k" :secret "s" :validate-hostnames false
                                  :db-name "agentic" :create-database? false})]
      (is (= :peer-server (:server-type cfg)))
      (is (= "localhost:8998" (:endpoint cfg)))
      (is (= "k" (:access-key cfg)))
      (is (= "s" (:secret cfg)))
      (is (false? (:validate-hostnames cfg)))
      ;; control keys are stripped from the client config
      (is (not (contains? cfg :db-name)))
      (is (not (contains? cfg :create-database?)))))
  (testing "Datomic Cloud forwards region/system/endpoint"
    (let [cfg (dat/client-config {:server-type :cloud :region "us-east-1" :system "prod"
                                  :endpoint "https://abc.execute-api.us-east-1.amazonaws.com"})]
      (is (= :cloud (:server-type cfg)))
      (is (= "us-east-1" (:region cfg)))
      (is (= "prod" (:system cfg)))))
  (testing ":client overrides everything verbatim"
    (let [raw {:server-type :peer-server :endpoint "h:1" :access-key "a" :secret "b"}]
      (is (= raw (dat/client-config {:client raw :db-name "x"}))))))

(defn- rm-rf [^java.io.File f]
  (when (.isDirectory f) (doseq [c (.listFiles f)] (rm-rf c)))
  (.delete f))

(deftest datomic-persists-to-disk-and-reconnects
  ;; the external-database shape: a durable store that survives reconnect — a second `datomic-stores`
  ;; opening the SAME db sees prior datoms, create-database is idempotent, schema re-transact is a no-op.
  (let [dir (str (System/getProperty "java.io.tmpdir") "/agentic-dat-" (System/nanoTime))
        db (str "persist-" (System/nanoTime))
        opts {:server-type :datomic-local :storage-dir dir :db-name db}]
    (try
      (if-let [s1 (try (dat/datomic-stores opts)
                       (catch Throwable t
                         (println "datomic-local file storage unavailable, skipping:" (.getMessage t))
                         nil))]
        (do
          (store/append (:conversation s1) "c1" {:role "user" :content "remember me"})
          (store/put-attribute (:conversation s1) "c1" "path" "payments")
          ;; reopen the same database with a fresh client/connection
          (let [s2 (dat/datomic-stores opts)]
            (is (= [{:role "user" :content "remember me"}] (store/history (:conversation s2) "c1")))
            (is (= "payments" (store/get-attribute (:conversation s2) "c1" "path")))
            (is (= 1 (store/message-count (:conversation s2) "c1")))))
        :skipped)
      (finally (rm-rf (java.io.File. dir))))))

;; --- live external Datomic (Pro peer-server), skip-if-absent — the established pattern ---

(deftest live-external-datomic-roundtrip
  (let [endpoint (System/getenv "AGENTIC_DATOMIC_ENDPOINT")]
    (if-not endpoint
      (println "AGENTIC_DATOMIC_ENDPOINT not set, skipping live external Datomic test")
      (let [{:keys [conversation]}
            (dat/datomic-stores {:server-type :peer-server
                                 :endpoint endpoint
                                 :access-key (System/getenv "AGENTIC_DATOMIC_ACCESS_KEY")
                                 :secret (System/getenv "AGENTIC_DATOMIC_SECRET")
                                 :validate-hostnames false
                                 :db-name (or (System/getenv "AGENTIC_DATOMIC_DB") "agentic")})
            cid (str "c-" (System/nanoTime))]
        (store/append conversation cid {:role "user" :content "hello external"})
        (is (= 1 (store/message-count conversation cid)))
        (is (= [{:role "user" :content "hello external"}] (store/history conversation cid)))))))
