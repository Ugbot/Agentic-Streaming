(ns agentic.suspend-test
  "Human-in-the-loop suspend/resume goldens — mirrors the jagentic-core HumanGate tests."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.core :as core]
            [agentic.event :as ev]
            [agentic.suspend :as sus]))

(def ^:private needs-approval?
  #(contains? (:metadata %) "needs_approval"))

(defn- approval-event []
  (ev/event "c1" "alice" "what is my balance?" {"needs_approval" "true"}))

(deftest approval-submit-suspends
  (testing "an approval-flagged turn suspends instead of completing"
    (let [svc (sus/suspension-service)
          g (sus/human-gate (core/banking-system) svc needs-approval?)
          res (sus/gate-submit g (approval-event) 0)]
      (is (= "awaiting-approval" (:path res)))
      (is (false? (:ok res)))
      (is (true? (sus/suspended? svc "c1")))
      (is (= [] (:tool-calls res))))))

(deftest resume-approved-replays-turn
  (testing "approved resume replays the held turn through the real graph"
    (let [svc (sus/suspension-service)
          g (sus/human-gate (core/banking-system) svc needs-approval?)]
      (sus/gate-submit g (approval-event) 0)
      (let [res (sus/gate-resume g "c1" true 10)]
        (is (= "payments" (:path res)))
        (is (re-find #"1234\.56" (:reply res)))
        (is (false? (sus/suspended? svc "c1")))))))

(deftest resume-denied-reports
  (testing "denied resume reports and clears the suspension"
    (let [svc (sus/suspension-service)
          g (sus/human-gate (core/banking-system) svc needs-approval?)]
      (sus/gate-submit g (approval-event) 0)
      (let [res (sus/gate-resume g "c1" false 10)]
        (is (= "denied" (:path res)))
        (is (false? (:ok res)))
        (is (false? (sus/suspended? svc "c1")))))))

(deftest normal-turn-passes-through
  (testing "a turn without the approval flag passes straight through the gate"
    (let [svc (sus/suspension-service)
          g (sus/human-gate (core/banking-system) svc needs-approval?)
          res (sus/gate-submit g (ev/event "c1" "alice" "what is my balance?") 0)]
      (is (= "payments" (:path res)))
      (is (true? (:ok res)))
      (is (false? (sus/suspended? svc "c1"))))))

(deftest timeout-escalates
  (testing "a suspension past the timeout escalates; before the timeout, nothing"
    (let [svc (sus/suspension-service)
          g (sus/human-gate (core/banking-system) svc needs-approval? 1000)]
      (sus/gate-submit g (approval-event) 0)
      (is (= [] (sus/gate-check-timeouts g 500)))
      (let [out (sus/gate-check-timeouts g 2000)]
        (is (= 1 (count out)))
        (is (= "escalated" (:path (first out))))
        (is (false? (sus/suspended? svc "c1")))))))

(deftest resume-nothing-pending
  (testing "resume with no pending suspension reports nothing pending"
    (let [svc (sus/suspension-service)
          g (sus/human-gate (core/banking-system) svc needs-approval?)
          res (sus/gate-resume g "c1" true 10)]
      (is (= "resume" (:path res)))
      (is (false? (:ok res))))))
