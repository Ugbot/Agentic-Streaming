(ns agentic.trace-test
  "Phase 7: observability. A recording tracer captures spans; the stream runtime opens one span per
   turn and per timer fire, carrying conversation/path/ok attrs and tool: events; the noop default
   keeps behaviour identical when no tracer is attached. Mirrors jagentic-core's trace goldens."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.core :as core]
            [agentic.event :as ev]
            [agentic.stream :as stream]
            [agentic.timers :as timers]
            [agentic.trace :as trace]))

(deftest recording-tracer-captures-spans
  (testing "start → attr → event → end records one span with name, attrs, events"
    (let [t (trace/recording-tracer)
          span (trace/start t "demo")]
      (-> span (trace/span-attr "k" "v") (trace/span-event "hello") (trace/span-end))
      (let [recorded (trace/spans t)]
        (is (= 1 (count recorded)))
        (let [s (first recorded)]
          (is (= "demo" (:name s)))
          (is (= "v" (get (:attrs s) "k")))
          (is (= ["hello"] (:events s))))
        (is (= ["demo"] (trace/names t)))))))

(deftest stream-traces-turns
  (testing "a traced stream-runtime opens one 'turn' span per event with conversation/path/ok + tools"
    (let [t (trace/recording-tracer)
          sr (-> (stream/stream-runtime (core/banking-system))
                 (stream/with-tracer t))]
      (stream/run sr (stream/seed-channel [(ev/event "c1" "alice" "what is my balance?")]))
      (is (= ["turn"] (trace/names t)))
      (let [s (first (trace/spans t))]
        (is (= "c1" (get (:attrs s) "conversation")))
        (is (= "payments" (get (:attrs s) "path")))
        (is (= "true" (get (:attrs s) "ok")))
        (is (some #{"tool:get_balance"} (:events s))
            "the balance turn invokes the get_balance tool")))))

(deftest timer-fire-traced
  (testing "firing a due timer opens a 'timer.fire' span carrying the payload's path"
    (let [t (trace/recording-tracer)
          sr (-> (stream/stream-runtime (core/banking-system))
                 (stream/with-tracer t))
          ts (timers/in-memory-timer-service)]
      (timers/schedule ts "follow-up" 1000 (ev/event "c1" "alice" "what is my balance?"))
      (stream/fire-due-timers sr ts 1000)
      (is (= ["timer.fire"] (trace/names t)))
      (is (= "payments" (get (:attrs (first (trace/spans t))) "path"))))))

(deftest noop-default-when-no-tracer
  (testing "no with-tracer → noop tracer, behaviour identical (no spans recorded, turns still run)"
    (let [sr (stream/stream-runtime (core/banking-system))
          results (stream/run sr (stream/seed-channel [(ev/event "c1" "alice" "hello there")]))]
      (is (= 1 (count results)))
      (is (= "general" (:path (first results)))))))
