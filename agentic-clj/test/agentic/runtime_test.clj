(ns agentic.runtime-test
  "Regression tests for the v1 semantics of the local runtime: single-writer ordering, turn_id
   idempotency, attempt-numbered retries, verification attempts, saga compensation, suspend/resume
   across a restart, replay of unknown events, and loader/backend selection."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [agentic.spec :as spec]
            [agentic.graph :as graph]
            [agentic.pipeline :as pipeline]
            [agentic.core :as core]
            [agentic.log :as log]))

(def support-yaml
  (.getPath (io/file (spec/spec-root) "conformance" "v1" "workflows" "support.yaml")))

(defn- support-system []
  (pipeline/load-system support-yaml))

(defn- turn [cid tid text]
  {:conversation-id cid :turn-id tid :user-id "u" :text text})

(defn- types [r] (mapv :type (:events r)))

(deftest single-writer-orders-concurrent-turns
  (let [sys (support-system)
        n 25
        turns (mapv #(turn "c1" (str "t" %) (str "question " % " about my balance")) (range n))
        ;; enqueue in arrival order from the caller; deliveries are in flight together.
        outcomes (mapv #(core/submit-async sys %) turns)
        results (mapv deref outcomes)]
    (testing "each turn observes exactly the turns that arrived before it"
      (is (= (range 1 (inc n)) (map #(get-in % [:state :turn-count]) results))))
    (testing "the conversation log is one dense sequence with no interleaving inside a turn"
      (let [events (core/events sys "c1")]
        (is (log/dense? events))
        (is (= (mapv :turn-id turns)
               (mapv :turn-id (filter #(= :turn-received (:type %)) events))))
        (is (= (mapv :turn-id turns)
               (distinct (map :turn-id events))))))
    (testing "conversations do not block each other"
      (is (= 1 (get-in (core/submit sys (turn "c2" "t1" "hello")) [:state :turn-count]))))))

(deftest duplicate-turn-answers-from-the-log
  (let [sys (support-system)
        first (core/submit sys (turn "c1" "t1" "what is my balance?"))
        before (count (core/events sys "c1"))
        again (core/submit sys (turn "c1" "t1" "what is my balance?"))]
    (is (= :completed (:status first)))
    (is (= :duplicate (:status again)))
    (is (false? (:ok again)))
    (is (= (:path first) (:path again)))
    (is (= (:reply first) (:reply again)))
    (is (= (:tool-calls first) (:tool-calls again)))
    (is (empty? (:events again)))
    (is (= before (count (core/events sys "c1"))) "no event is appended for a redelivery")))

(def retry-workflow
  {"spec_version" "agentic/v1" "backend" "local"
   "agent" {"id" "retrying"
            "router" {"kind" "keyword" "default" "main" "rules" {"main" ["charge"]}}
            "paths" {"main" {"brain" "rule" "prompt" "p" "tool_triggers" {"charge" "flaky"}}}
            "verifier" {"kind" "none"}}
   "policies" {"retry" {"kind" "fixed" "max_attempts" 3 "initial_delay_ms" 0 "jitter" false}}
   "tools" [{"id" "flaky" "kind" "failing" "x-fail-attempts" 2 "value" "ok"}]})

(deftest retries-are-attempt-numbered
  (let [sys (pipeline/system-from retry-workflow (pipeline/open-stores (spec/load-workflow retry-workflow)))
        r (core/submit sys (turn "c1" "t1" "this charge"))]
    (is (= :completed (:status r)))
    (is (= [[0 1 true] [0 2 true] [0 3 false]]
           (mapv (juxt :index :attempt #(some? (:error %))) (:tool-calls r))))
    (is (= [:tool-failed :tool-failed :tool-called]
           (filterv #{:tool-failed :tool-called} (types r)))))
  (testing "exhausted retries fail the turn with error class tool"
    (let [wf (assoc-in retry-workflow ["tools" 0 "x-fail-attempts"] 5)
          sys (pipeline/system-from wf (pipeline/open-stores (spec/load-workflow wf)))
          r (core/submit sys (turn "c1" "t1" "this charge"))]
      (is (= :failed (:status r)))
      (is (= :tool (get-in r [:error :class])))
      (is (= 3 (count (:tool-calls r))))))
  (testing "on_tool_error: continue lets the brain answer without the tool"
    (let [wf (-> retry-workflow
                 (assoc-in ["tools" 0 "x-fail-attempts"] 5)
                 (assoc-in ["policies" "on_tool_error"] "continue"))
          sys (pipeline/system-from wf (pipeline/open-stores (spec/load-workflow wf)))
          r (core/submit sys (turn "c1" "t1" "this charge"))]
      (is (= :completed (:status r)))
      (is (= 3 (count (filter :error (:tool-calls r))))))))

(def verify-workflow
  {"spec_version" "agentic/v1" "backend" "local"
   "agent" {"id" "strict"
            "router" {"kind" "keyword" "default" "main"}
            "paths" {"main" {"brain" "rule" "prompt" "p"}}
            "verifier" {"kind" "regex" "pattern" "^never-matches$"}}
   "policies" {"verification" {"max_attempts" 2 "on_exhausted" "unverified"}}})

(deftest verification-attempts-are-bounded
  (let [sys (pipeline/system-from verify-workflow (pipeline/open-stores (spec/load-workflow verify-workflow)))
        r (core/submit sys (turn "c1" "t1" "hello"))]
    (is (= :unverified (:status r)))
    (is (= 2 (count (filter #{:verification-failed} (types r)))))
    (is (= 2 (count (filter #{:reply-drafted} (types r)))))
    (is (string? (:reply r)) "the last draft is reported so the caller can see what was rejected")
    (is (= :verification (get-in r [:error :class]))))
  (testing "on_exhausted: fail"
    (let [wf (assoc-in verify-workflow ["policies" "verification" "on_exhausted"] "fail")
          sys (pipeline/system-from wf (pipeline/open-stores (spec/load-workflow wf)))]
      (is (= :failed (:status (core/submit sys (turn "c1" "t1" "hello"))))))))

(defn- rnd [prefix] (str prefix (subs (str (java.util.UUID/randomUUID)) 0 8)))

(defn- path-verifier-workflow
  "Four paths routed by their own name; the agent-level regex accepts replies from `rejecting`
   and `fallback-ok` only. `rejecting` carries a never-matching regex, `lenient` a prefix verifier."
  [[rejecting lenient fallback-ok _fallback-ko :as names] max-attempts]
  {"spec_version" "agentic/v1" "backend" "local"
   "agent" {"id" (rnd "verifiers-")
            "router" {"kind" "keyword" "default" rejecting
                      "rules" (into {} (map (fn [n] [n [n]]) names))}
            "paths" {rejecting {"brain" "rule" "prompt" "p"
                                "verifier" {"kind" "regex" "pattern" (str "^" (rnd "never-"))}}
                     lenient {"brain" "rule" "prompt" "p" "verifier" {"kind" "prefix"}}
                     fallback-ok {"brain" "rule" "prompt" "p"}
                     _fallback-ko {"brain" "rule" "prompt" "p"}}
            "verifier" {"kind" "regex" "pattern" (str "^\\[(" rejecting "|" fallback-ok ")\\]")}}
   "policies" {"verification" {"max_attempts" max-attempts "on_exhausted" "unverified"}}})

(deftest path-verifier-overrides-agent-verifier-and-absent-falls-back
  (let [names (mapv rnd ["audit" "chat" "billing" "account"])
        [rejecting lenient fallback-ok fallback-ko] names
        max-attempts (+ 2 (rand-int 3))
        wf (path-verifier-workflow names max-attempts)
        sys (pipeline/system-from wf (pipeline/open-stores (spec/load-workflow wf)))
        failed-count (fn [r] (count (filter #{:verification-failed} (types r))))]
    (testing "the path's regex judges, not the agent's"
      (let [r (core/submit sys (turn "c1" "t1" rejecting))]
        (is (= :unverified (:status r)))
        (is (= rejecting (:path r)))
        (is (= :verification (get-in r [:error :class])))
        (is (= max-attempts (failed-count r)))))
    (testing "the path's prefix accepts what the agent's regex rejects"
      (let [r (core/submit sys (turn "c1" "t2" lenient))]
        (is (= :completed (:status r)))
        (is (str/starts-with? (:reply r) (str "[" lenient "]")))
        (is (zero? (failed-count r)))))
    (testing "a path without a verifier uses agent.verifier"
      (is (= :completed (:status (core/submit sys (turn "c1" "t3" fallback-ok)))))
      (let [r (core/submit sys (turn "c1" "t4" fallback-ko))]
        (is (= :unverified (:status r)))
        (is (= fallback-ko (:path r)))
        (is (= max-attempts (failed-count r)))))))

(deftest path-verifier-none-disables-verification-and-absent-defaults-to-prefix
  (let [open-path (rnd "open") plain (rnd "plain")
        base {"spec_version" "agentic/v1" "backend" "local"
              "agent" {"id" (rnd "a-")
                       "router" {"kind" "keyword" "default" plain
                                 "rules" {open-path [open-path] plain [plain]}}
                       "paths" {open-path {"brain" "rule" "prompt" "p" "verifier" {"kind" "none"}}
                                plain {"brain" "rule" "prompt" "p"}}}
              "policies" {"verification" {"max_attempts" 1 "on_exhausted" "unverified"}}}
        strict (assoc-in base ["agent" "verifier"] {"kind" "regex" "pattern" (str "^" (rnd "never-"))})
        sys (pipeline/system-from strict (pipeline/open-stores (spec/load-workflow strict)))]
    (testing "kind: none on the path wins over a rejecting agent.verifier"
      (is (= :completed (:status (core/submit sys (turn "c1" "t1" open-path)))))
      (is (= :unverified (:status (core/submit sys (turn "c1" "t2" plain))))))
    (testing "neither declared: the path verifies with the default prefix"
      (let [g (:graph (pipeline/build base))
            v (graph/verifier-for g plain)]
        (is (fn? v))
        (is (= [true (str "[" plain "] x")] (v (str "[" plain "] x") nil)))
        (is (false? (first (v (rnd "x") nil))))
        (is (true? (first ((graph/verifier-for g open-path) (rnd "x") nil))) "kind: none accepts anything")
        (is (= :completed (:status (core/submit (pipeline/system-from base (pipeline/open-stores (spec/load-workflow base)))
                                                (turn "c1" "t1" plain)))))))
    (testing "a broken path verifier is reported at the path's location"
      (let [broken (assoc-in base ["agent" "paths" plain "verifier"] {"kind" "regex"})
            e (try (pipeline/build broken) nil (catch clojure.lang.ExceptionInfo e e))]
        (is (some? e))
        (is (= ["agent" "paths" plain "verifier" "pattern"] (:path (ex-data e))) (pr-str (ex-data e)))))))

(def saga-workflow
  {"spec_version" "agentic/v1" "backend" "local"
   "agent" {"id" "sagas" "router" {"kind" "keyword" "default" "main"}
            "paths" {"main" {"brain" "rule" "prompt" "p"}} "verifier" {"kind" "none"}}
   "policies" {"on_tool_error" "fail" "retry" {"kind" "none" "max_attempts" 1}}
   "tools" [{"id" "a" "kind" "constant" "value" 1 "compensation" "undo_a"}
            {"id" "b" "kind" "constant" "value" 2}
            {"id" "undo_b" "kind" "constant" "value" -2}
            {"id" "undo_a" "kind" "constant" "value" -1}
            {"id" "boom" "kind" "failing"}]
   "saga" {"steps" [{"name" "first" "tool" "a"}
                    {"name" "second" "tool" "b" "compensate_with" "undo_b"}
                    {"name" "third" "tool" "boom"}]}})

(deftest saga-compensates-in-reverse-order
  (let [sys (pipeline/system-from saga-workflow (pipeline/open-stores (spec/load-workflow saga-workflow)))
        r (core/submit sys (turn "c1" "t1" "go"))]
    (is (= :failed (:status r)))
    (is (= :tool (get-in r [:error :class])))
    (is (= ["a" "b" "boom" "undo_b" "undo_a"] (mapv :tool (:tool-calls r))))
    (is (= [0 1 2 3 4] (mapv :index (:tool-calls r))))
    (is (= [:compensation-started :compensation-step :compensation-step :compensation-completed :turn-failed]
           (filterv #{:compensation-started :compensation-step :compensation-completed :turn-failed} (types r)))))
  (testing "a saga that runs through completes"
    (let [wf (update-in saga-workflow ["saga" "steps"] pop)
          sys (pipeline/system-from wf (pipeline/open-stores (spec/load-workflow wf)))
          r (core/submit sys (turn "c1" "t1" "go"))]
      (is (= :completed (:status r)))
      (is (= ["a" "b"] (mapv :tool (:tool-calls r)))))))

(def suspend-workflow
  {"spec_version" "agentic/v1" "backend" "local"
   "agent" {"id" "approval" "router" {"kind" "keyword" "default" "main"}
            "paths" {"main" {"brain" "rule" "prompt" "p" "x-suspend-until" "approval"}}
            "verifier" {"kind" "none"}}})

(deftest suspended-turn-survives-restart-and-resumes-on-signal
  (let [wf (spec/load-workflow suspend-workflow)
        stores (pipeline/open-stores wf)
        sys (pipeline/system-from wf stores)
        r1 (core/submit sys (turn "c1" "t1" "refund please"))
        restarted (pipeline/system-from wf (select-keys sys [:store :state :log]))
        r2 (core/submit restarted (assoc (turn "c1" "t1" "") :signal {"kind" "approval" "approved" true}))]
    (is (= :suspended (:status r1)))
    (is (= [:turn-received :routed :turn-suspended] (types r1)))
    (is (= :completed (:status r2)))
    (is (= [:turn-resumed :brain-started :reply-drafted :memory-written :turn-completed] (types r2)))
    (is (= "main" (:path r2)))
    (is (= 1 (get-in r2 [:state :turn-count])) "the resumption is not a new turn")
    (testing "a redelivery of the original turn after resumption is a duplicate, not a re-execution"
      (is (= :duplicate (:status (core/submit restarted (turn "c1" "t1" "refund please"))))))))

(deftest unknown-events-are-preserved-and-ignored-by-the-fold
  (let [events [{:sequence 0 :turn-id "t1" :type :turn-received :payload {:turn-id "t1"}}
                {:sequence 1 :turn-id "t1" :type :engine-checkpoint :payload {:offset 42}}
                {:sequence 2 :turn-id "t1" :type :turn-completed :payload {:reply "ok"}}]]
    (is (log/dense? events))
    (is (= {:turn-count 1 :transcript-length 0} (log/reduce-state events)))
    (let [r (log/turn-result "c1" "t1" events)]
      (is (= :completed (:status r)))
      (is (= [:turn-received :engine-checkpoint :turn-completed] (mapv :type (:events r)))))))

(defn- validation-message [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e
         (if (spec/validation-error? e) (ex-message e) (throw e)))))

(deftest loader-rejects-what-it-cannot-run
  (let [base (spec/read-document support-yaml)]
    (testing "an unknown backend is an error, never a fallback to local"
      (is (re-find #"backend" (validation-message #(pipeline/build (assoc base "backend" "celery"))))))
    (testing "an unknown store kind is an error"
      (is (re-find #"store kind" (validation-message
                                  #(pipeline/open-stores (spec/load-workflow (assoc base "stores" {"conversation" {"kind" "redis"}})))))))
    (testing "an unreachable datomic store fails the load..."
      (let [unreachable {"kind" "datomic" "server-type" "peer-server" "endpoint" "localhost:1"
                         "access-key" "k" "secret" "s" "validate-hostnames" false}]
        (is (re-find #"unreachable" (validation-message
                                     #(pipeline/open-stores (spec/load-workflow (assoc base "stores" {"conversation" unreachable}))))))
        (testing "...unless it asks to degrade"
          (let [stores (pipeline/open-stores
                        (spec/load-workflow (assoc base "stores" {"conversation" (assoc unreachable "on_unavailable" "degrade")})))]
            (is (satisfies? log/EventLog (:log stores)))))))
    (testing "unknown keys outside x- and runtime: are rejected; extensions are kept"
      (is (re-find #"surprise" (validation-message #(spec/load-workflow (assoc base "surprise" 1)))))
      (let [wf (spec/load-workflow (assoc base "x-owner" "clj" "runtime" {"clojure" {"threads" 2}}))]
        (is (= "clj" (:x-owner wf)))
        (is (= {"threads" 2} (get-in wf [:runtime "clojure"])))))
    (testing "the seven rules"
      (is (re-find #"path" (validation-message #(spec/load-workflow (assoc-in base ["agent" "router" "default"] "nowhere")))))
      (is (re-find #"tool" (validation-message #(spec/load-workflow (assoc-in base ["agent" "paths" "billing" "tools"] ["ghost"])))))
      (is (re-find #"llm" (validation-message #(spec/load-workflow (-> base (dissoc "llm") (assoc-in ["agent" "paths" "billing" "brain"] "llm"))))))
      (is (re-find #"dim" (validation-message #(spec/load-workflow (assoc base "embeddings" {"dim" 64} "retrieval" {"dim" 128 "kb" []})))))
      (is (re-find #"compensate_with" (validation-message #(spec/load-workflow (assoc base "saga" {"steps" [{"tool" "lookup_charge" "compensate_with" "ghost"}]})))))
      (is (re-find #"unique|duplicate" (validation-message #(spec/load-workflow (update base "tools" conj (first (get base "tools"))))))))))

(deftest edn-and-yaml-load-to-the-same-workflow
  (let [from-yaml (spec/load-path support-yaml)
        edn-file (java.io.File/createTempFile "support" ".edn")]
    (spit edn-file (spec/->edn-string from-yaml))
    (is (= from-yaml (spec/load-path (.getPath edn-file))))
    (is (= "agentic/v1" (:spec-version from-yaml)))
    (is (= "agentic/v1" (get (spec/->wire from-yaml) "spec_version")))))
