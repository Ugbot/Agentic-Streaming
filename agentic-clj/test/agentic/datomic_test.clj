(ns agentic.datomic-test
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.store :as store]
            [agentic.store.datomic :as dat]
            [agentic.core :as core]
            [agentic.event :as ev]
            [agentic.banking :as banking]
            [agentic.log]
            [agentic.spec]
            [agentic.pipeline]
            [clojure.java.io]
            [datomic.client.api]))

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

(deftest datomic-concurrent-appends-are-dense
  ;; Regression for the read-modify-write race: many writers appending to one conversation at once
  ;; must produce positions 0..n-1 with no gap or duplicate, for the transcript and for the event log.
  (when-let [{:keys [conn conversation log]} (try-stores)]
    (let [n 40
          writers (mapv (fn [i] (future (store/append conversation "race" {:role "user" :content (str i)})))
                        (range n))]
      (run! deref writers)
      (is (= n (count (store/history conversation "race"))))
      (is (= (range n)
             (sort (map first (datomic.client.api/q '[:find ?pos :in $ ?cid
                                                     :where [?m :message/conversation ?cid] [?m :message/position ?pos]]
                                                   (datomic.client.api/db conn) "race"))))))
    (let [n 40
          writers (mapv (fn [i] (future (agentic.log/append-event! log "race"
                                                                   {:turn-id (str "t" i) :type :turn-received
                                                                    :payload {:i i}})))
                        (range n))
          appended (mapv deref writers)]
      (is (= (range n) (sort (map :sequence appended))))
      (is (agentic.log/dense? (agentic.log/conversation-events log "race"))))))

(deftest datomic-event-log-time-travel
  (when-let [{:keys [conn log]} (try-stores)]
    (agentic.log/append-event! log "c1" {:turn-id "t1" :type :turn-received :payload {}})
    (let [t (dat/basis-t conn)]
      (agentic.log/append-event! log "c1" {:turn-id "t1" :type :turn-completed :payload {:reply "ok"}})
      (is (= 2 (count (agentic.log/conversation-events log "c1"))))
      (is (= [:turn-received] (mapv :type (dat/events-as-of conn "c1" t)))))))

(deftest datomic-backed-runtime-runs-the-support-workflow
  (when (try-stores)
    (let [wf (assoc (agentic.spec/read-document
                     (.getPath (clojure.java.io/file (agentic.spec/spec-root) "conformance" "v1" "workflows" "support.yaml")))
                    "stores" {"conversation" {"kind" "datomic" "db_name" (str "rt-" (System/nanoTime)) "storage-dir" "mem"}})
          canonical (agentic.spec/load-workflow wf)
          stores (agentic.pipeline/open-stores canonical)
          sys (agentic.pipeline/system-from canonical stores)
          r (core/submit sys {:conversation-id "c1" :turn-id "t1" :user-id "u" :text "what is my balance?"})]
      (is (= :completed (:status r)))
      (is (= "billing" (:path r)))
      (is (= ["lookup_charge"] (mapv :tool (:tool-calls r))))
      (is (= :duplicate (:status (core/submit sys {:conversation-id "c1" :turn-id "t1" :user-id "u" :text "again"}))))
      (testing "a restart over the same datomic log replays the state"
        (let [again (agentic.pipeline/system-from canonical stores)
              r2 (core/submit again {:conversation-id "c1" :turn-id "t2" :user-id "u" :text "I lost my password"})]
          (is (= 2 (get-in r2 [:state :turn-count])))
          (is (= [] (:tool-calls r2))))))))
