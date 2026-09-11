(ns agentic.conformance-test
  "Runs the shared v1 fixtures from spec/conformance/v1/fixtures against this runtime. Every fixture
   must pass or be an honest skip; a skip is reported, never counted as a pass."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.conformance :as conf]))

(deftest shared-fixtures
  (let [outcomes (conf/run-all)]
    (is (= 15 (count outcomes)) "every fixture file in the spec is exercised")
    (doseq [{:keys [id status problems]} outcomes]
      (testing id
        (case status
          :passed (is true)
          :skipped (println "SKIP" id (first problems))
          :failed (is (empty? problems) (str id " " (pr-str problems))))))
    (println (conf/report outcomes))))

(deftest comparison-rules
  (testing "events_include is an ordered subsequence, extras allowed"
    (let [actual {"events" [{"type" "turn_received"} {"type" "routed"} {"type" "x"} {"type" "turn_completed"}]}]
      (is (empty? (conf/check-expectation {"events_include" ["turn_received" "turn_completed"]} actual)))
      (is (seq (conf/check-expectation {"events_include" ["turn_completed" "routed"]} actual)))
      (is (seq (conf/check-expectation {"events_exclude" ["routed"]} actual)))))
  (testing "tool_calls compares tool, order and the named keys; failed means an error is recorded"
    (let [actual {"tool_calls" [{"tool" "a" "index" 0 "attempt" 1 "error" {"class" "tool"}}
                                {"tool" "a" "index" 0 "attempt" 2}]}]
      (is (empty? (conf/check-expectation
                   {"tool_calls" [{"tool" "a" "index" 0 "failed" true} {"tool" "a" "attempt" 2}]} actual)))
      (is (seq (conf/check-expectation {"tool_calls" [{"tool" "a"} {"tool" "a" "failed" true}]} actual)))
      (is (seq (conf/check-expectation {"tool_calls" [{"tool" "a"}]} actual)))))
  (testing "state_includes is a subset check, reply_matches a regex search"
    (is (empty? (conf/check-expectation {"state_includes" {"turn_count" 2} "reply_matches" "bal"}
                                        {"state" {"turn_count" 2 "extra" 1} "reply" "your balance"})))
    (is (seq (conf/check-expectation {"reply_matches" "^balance"} {"reply" "your balance"})))))

(deftest unsupported-requirements-skip
  (is (= ["time_travel_ui"] (conf/unsupported {"requires" ["tools" "time_travel_ui"]})))
  (is (= [] (conf/unsupported {"requires" ["tools" "saga"]}))))
