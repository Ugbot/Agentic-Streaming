(ns agentic.cep-weave-test
  "Phase B (HEADLINE weave): the declarative `cep:` section compiled into runnable wirings and woven
   into the pipeline submit path. Mirror of jagentic-core's CepSpec/CepWiring behaviour — compiler,
   recursion guard, condition mini-language, and the end-to-end incident.yaml escalation."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.cep :as cep]
            [agentic.pipeline :as pipeline]
            [agentic.tools :as tools]
            [agentic.store :as store]
            [agentic.graph :as graph]
            [agentic.event :refer [event]]))

(def ^:private five-min (* 5 60 1000))

(defn- incident-submit-spec
  "The incident rule from incident.yaml, expressed as a compile-cep spec (string keys): 3× anomalies
   on one host within 5 min, keyed by conversation_id, ts from metadata.ts, submitting a derived event."
  []
  {"name" "host_incident"
   "key" "conversation_id"
   "ts" "metadata.ts"
   "within" five-min
   "pattern" [{"stage" "first"  "where" {"text_contains" "anomaly"}}
              {"stage" "second" "where" {"text_contains" "anomaly"} "contiguity" "followedBy"}
              {"stage" "third"  "where" {"text_contains" "anomaly"} "contiguity" "followedBy"}]
   "on_match" {"kind" "submit" "text" "incident on {key}"}})

(deftest submit-fires-once-and-recursion-guarded
  (testing "3 anomalies fire exactly one derived submit; feeding the derived event back fires nothing"
    (let [[w] (cep/compile-cep [(incident-submit-spec)])
          submitted (atom [])
          submit-fn #(swap! submitted conj %)
          tools-reg (tools/registry)
          feed (fn [ts] (cep/cep-on-event w (event "h1" "monitor" "anomaly cpu" {"ts" (str ts)})
                                          submit-fn tools-reg))]
      (feed 0)
      (is (= 0 (count @submitted)) "first anomaly: nothing submitted")
      (feed 60000)
      (is (= 0 (count @submitted)) "second anomaly: still nothing")
      (feed 120000)
      (is (= 1 (count @submitted)) "third anomaly within the bound: exactly one derived submit")
      (let [derived (first @submitted)]
        (is (= "incident on h1" (:text derived)) "{key} substituted with the conversation id")
        (is (= "h1" (:conversation-id derived)) "derived event keyed by the match key")
        (is (= "true" (get-in derived [:metadata cep/derived-key])) "derived event tagged DERIVED"))
      ;; Recursion guard: feeding the derived (DERIVED-tagged) event back must add nothing.
      (cep/cep-on-event w (first @submitted) submit-fn tools-reg)
      (is (= 1 (count @submitted)) "derived events are skipped — no recursion"))))

(deftest tool-action-fires-once
  (testing "on_match kind=tool executes the tool exactly once on a completed match"
    (let [calls (atom [])
          tools-reg (tools/registry)
          _ (tools/register tools-reg "open_ticket" "Open a ticket"
                            (fn [params] (swap! calls conj params) "TICKET-OPENED"))
          spec (assoc (incident-submit-spec)
                      "on_match" {"kind" "tool" "tool" "open_ticket" "args" {"sev" "high"}})
          [w] (cep/compile-cep [spec])
          submit-fn (fn [_] (throw (ex-info "submit must not be called for a tool action" {})))
          feed (fn [ts] (cep/cep-on-event w (event "h1" "monitor" "anomaly cpu" {"ts" (str ts)})
                                          submit-fn tools-reg))]
      (feed 0) (feed 60000) (feed 120000)
      (is (= 1 (count @calls)) "open_ticket called exactly once")
      (is (= {"sev" "high"} (first @calls)) "called with the declared args"))))

(deftest condition-mini-language
  (testing "metadata_gt and metadata_equals compile to the documented predicates"
    (let [gt-cond (#'cep/spec-condition {"metadata_gt" {"score" 0.9}})
          eq-cond (#'cep/spec-condition {"metadata_equals" {"region" "eu"}})]
      (is (true? (gt-cond (event "k" "u" "x" {"score" "0.95"}) []))
          "metadata_gt: 0.95 > 0.9")
      (is (false? (gt-cond (event "k" "u" "x" {"score" "0.5"}) []))
          "metadata_gt: 0.5 not > 0.9")
      (is (false? (gt-cond (event "k" "u" "x" {}) []))
          "metadata_gt: missing/unparseable metadata is guarded to false")
      (is (true? (eq-cond (event "k" "u" "x" {"region" "eu"}) []))
          "metadata_equals: matching value")
      (is (false? (eq-cond (event "k" "u" "x" {"region" "us"}) []))
          "metadata_equals: non-matching value"))))

(deftest text-contains-list-and-any
  (testing "text_contains accepts a single needle or a list; any/nil matches everything"
    (let [single (#'cep/spec-condition {"text_contains" "anomaly"})
          many (#'cep/spec-condition {"text_contains" ["error" "anomaly"]})
          any-where (#'cep/spec-condition "any")
          nil-where (#'cep/spec-condition nil)]
      (is (true? (single (event "k" "u" "an anomaly here" {}) [])))
      (is (false? (single (event "k" "u" "nothing wrong" {}) [])))
      (is (true? (many (event "k" "u" "an anomaly" {}) [])))
      (is (true? (many (event "k" "u" "an error" {}) [])))
      (is (false? (many (event "k" "u" "all good" {}) [])))
      (is (true? (any-where (event "k" "u" "whatever" {}) [])))
      (is (true? (nil-where (event "k" "u" "whatever" {}) []))))))

(deftest incident-yaml-integration-escalates
  (testing "loading incident.yaml and feeding 3 anomalies for one host escalates that conversation"
    (let [sys (pipeline/load-system "../examples/pipelines/incident.yaml")
          host "host-7"
          feed (fn [ts] (pipeline/submit sys (event host "monitor" "anomaly: cpu high"
                                                    {"ts" (str ts)})))]
      (is (seq (:cep sys)) "the cep: section compiled into at least one wiring")
      (feed 0) (feed 60000) (feed 120000)
      ;; The 3rd anomaly fires a derived "incident: ..." submit, which the keyword router sends to the
      ;; escalate path; the graph records the chosen path under the "path" attribute.
      (is (= "escalate" (store/get-attribute (:store sys) host graph/path-attr))
          "the host's conversation last routed to the escalate path"))))
