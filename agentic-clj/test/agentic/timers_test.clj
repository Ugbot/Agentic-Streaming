(ns agentic.timers-test
  "Phase 2: portable timers. Logical time, deterministic ordering, durable-via-store, and firing due
   timers back through the stream runtime as turns. Mirrors jagentic-core's timer goldens."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [agentic.core :as core]
            [agentic.event :as ev]
            [agentic.store :as store]
            [agentic.stream :as stream]
            [agentic.timers :as timers]))

(deftest schedule-advance-and-next-deadline
  (testing "due timers come out ascending by :fire-at; advance removes them"
    (let [ts (timers/in-memory-timer-service)]
      (timers/schedule ts "a" 100 (ev/event "c" "u" "a"))
      (timers/schedule ts "b" 50 (ev/event "c" "u" "b"))
      (timers/schedule ts "c" 200 (ev/event "c" "u" "c"))
      (is (= 50 (timers/next-deadline ts)))
      (is (= ["b" "a"] (mapv :id (timers/advance-to ts 150))))
      (is (= 200 (timers/next-deadline ts)))
      (is (= [] (timers/advance-to ts 150)) "already-fired timers do not re-fire"))))

(deftest equal-deadlines-keep-schedule-order
  (testing "equal :fire-at preserves insertion order as the stable tie-break"
    (let [ts (timers/in-memory-timer-service)]
      (timers/schedule ts "x" 100 (ev/event "c" "u" "x"))
      (timers/schedule ts "y" 100 (ev/event "c" "u" "y"))
      (timers/schedule ts "z" 100 (ev/event "c" "u" "z"))
      (is (= ["x" "y" "z"] (mapv :id (timers/advance-to ts 100)))))))

(deftest cancel-removes-pending
  (testing "cancel returns truthy once, falsey thereafter; cancelled timer never fires"
    (let [ts (timers/in-memory-timer-service)]
      (timers/schedule ts "a" 100 (ev/event "c" "u" "a"))
      (is (timers/cancel ts "a"))
      (is (not (timers/cancel ts "a")))
      (is (= [] (timers/advance-to ts 1000))))))

(deftest durable-survives-restore
  (testing "a scheduled timer survives a fresh service over the same store"
    (let [kv (store/in-memory-keyed-state-store)
          dts (timers/durable-timer-service kv)]
      (timers/schedule dts "escalate" 500 (ev/event "c9" "alice" "what is my balance?"))
      ;; A NEW durable service over the SAME store, then restore.
      (let [restored (timers/restore (timers/durable-timer-service kv))]
        (is (= 500 (timers/next-deadline restored)))
        (let [due (timers/advance-to restored 500)]
          (is (= 1 (count due)))
          (is (= "c9" (get-in (first due) [:payload :conversation-id])))
          (is (= "what is my balance?" (get-in (first due) [:payload :text]))))))))

(deftest fire-due-timers-through-stream
  (testing "due timer payloads fire back through the stream runtime as turns"
    (let [sr (stream/stream-runtime (core/banking-system))
          ts (timers/in-memory-timer-service)]
      (timers/schedule ts "followup" 1000 (ev/event "c1" "alice" "what is my balance?"))
      (is (= [] (stream/fire-due-timers sr ts 999)) "nothing due before the deadline")
      (let [results (stream/fire-due-timers sr ts 1000)]
        (is (= 1 (count results)))
        (is (= "payments" (:path (first results))))
        (is (str/includes? (:reply (first results)) "1234.56"))))))
