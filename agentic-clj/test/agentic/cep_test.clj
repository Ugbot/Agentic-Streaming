(ns agentic.cep-test
  "Phase 4 (HEADLINE): portable keyed CEP NFA matcher. Mirrors jagentic-core's
   org.jagentic.core.cep.* goldens — behaviour at parity."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.cep :as cep]
            [agentic.event :refer [event]]))

(def ^:private five-min (* 5 60 1000))

(defn- incident-pattern []
  (-> (cep/begin "first" cep/any-cond)
      (cep/followed-by "second" cep/any-cond)
      (cep/followed-by "third" cep/any-cond)
      (cep/within five-min)))

(defn- anomaly [host]
  (event host "monitor" "anomaly"))

(deftest three-stage-relaxed-completes
  (testing "three anomalies on one host complete the incident pattern; named keys in stage order"
    (let [m (cep/cep-matcher (incident-pattern))
          h1 (anomaly "h1")]
      (is (= [] (cep/cep-match m "h1" 0 h1)) "first event: no completion")
      (is (= [] (cep/cep-match m "h1" 60000 h1)) "second event: still partial")
      (let [matches (cep/cep-match m "h1" 120000 h1)]
        (is (= 1 (count matches)) "third event completes exactly one match")
        (let [match (first matches)]
          (is (= 3 (count (:events match))) "three events in the match")
          (is (= ["first" "second" "third"] (vec (keys (:named match))))
              "named keys in stage order"))))))

(deftest keys-are-independent
  (testing "partials are isolated per key"
    (let [m (cep/cep-matcher (incident-pattern))
          h1 (anomaly "h1")
          h2 (anomaly "h2")]
      (is (= [] (cep/cep-match m "h2" 0 h2)))
      (is (= [] (cep/cep-match m "h2" 60000 h2)) "h2 has only two — no completion")
      (is (= [] (cep/cep-match m "h1" 0 h1)))
      (is (= [] (cep/cep-match m "h1" 60000 h1)))
      (is (= 1 (count (cep/cep-match m "h1" 120000 h1)))
          "h1 reaching three completes, unaffected by h2"))))

(deftest within-expiry
  (testing "partials older than `within` expire; flush-expired surfaces their events"
    (let [m (cep/cep-matcher (incident-pattern))
          h1 (anomaly "h1")]
      (cep/cep-match m "h1" 0 h1)
      (cep/cep-match m "h1" 60000 h1)
      (is (= [] (cep/cep-match m "h1" (inc five-min) h1))
          "the third event arrives after the bound — partials expired, no completion")
      (is (>= (count (cep/flush-expired m "h1" (* 100 five-min))) 1)
          "flush-expired returns at least one timed-out partial's events"))))

(deftest strict-next-drops-on-gap
  (testing ":next is strict — an intervening non-match drops the partial"
    (let [m (cep/cep-matcher
             (-> (cep/begin "a" (cep/simple-cond #(= (:text %) "a")))
                 (cep/pnext "b" (cep/simple-cond #(= (:text %) "b")))))
          ev (fn [t] (event "k" "u" t))]
      (is (= [] (cep/cep-match m "k" 0 (ev "a"))) "partial at stage 0")
      (is (= [] (cep/cep-match m "k" 1 (ev "x"))) "strict gap drops the partial")
      (is (= [] (cep/cep-match m "k" 2 (ev "b"))) "b cannot complete — partial gone"))))

(deftest relaxed-followed-by-skips-gap
  (testing ":followed-by is relaxed — a non-match is skipped, the partial waits"
    (let [m (cep/cep-matcher
             (-> (cep/begin "a" (cep/simple-cond #(= (:text %) "a")))
                 (cep/followed-by "b" (cep/simple-cond #(= (:text %) "b")))))
          ev (fn [t] (event "k" "u" t))]
      (is (= [] (cep/cep-match m "k" 0 (ev "a"))) "partial at stage 0")
      (is (= [] (cep/cep-match m "k" 1 (ev "x"))) "relaxed skip — partial survives")
      (is (= 1 (count (cep/cep-match m "k" 2 (ev "b")))) "b completes the match"))))

(deftest iterative-condition-same-host
  (testing "an iterative condition inspects matched-so-far (same conversation-id as the first)"
    (let [m (cep/cep-matcher
             (-> (cep/begin "a" cep/any-cond)
                 (cep/followed-by "b"
                                  (fn [e so-far]
                                    (= (:conversation-id e)
                                       (:conversation-id (first so-far)))))))
          e1 (anomaly "h1")
          e2 (anomaly "h1")]
      (is (= [] (cep/cep-match m "h1" 0 e1)))
      (is (= 1 (count (cep/cep-match m "h1" 1 e2)))
          "second same-host event satisfies the iterative condition and completes"))))

(deftest cep-observer-fires-once
  (testing "cep-observer drives the matcher and calls on-match per completed match"
    (let [fired (atom [])
          obs (cep/cep-observer (incident-pattern)
                                :conversation-id
                                (constantly 0) ; all within the bound
                                #(swap! fired conj %))
          h1 (anomaly "h1")]
      (obs h1)
      (obs h1)
      (obs h1)
      (is (= 1 (count @fired)) "observer fired exactly once for three anomalies")
      (is (= 3 (count (:events (first @fired)))) "the fired match has three events"))))
