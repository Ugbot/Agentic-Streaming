(ns agentic.workflow-timers-test
  "Workflow timers (spec section 8) on the Clojure runtime: the manual processing clock, the
   watermark, scheduling on the first turn, firing at the head of the first due turn with the tool's
   payload, exactly-once firing, and recovery of pending timers and the clock from the log across a
   restart. Deadlines are randomized."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.spec :as spec]
            [agentic.pipeline :as pipeline]
            [agentic.core :as core]
            [agentic.log :as log]
            [agentic.workflow-timers :as wt]))

(defn- rnd [lo hi] (+ lo (rand-int (- hi lo))))

(defn- workflow [clock after-ms payload]
  {"spec_version" "agentic/v1" "backend" "local"
   "timers" [{"id" "followup" "after_ms" after-ms "clock" clock "tool" "notify" "payload" payload}]
   "agent" {"id" "timers"
            "router" {"kind" "keyword" "default" "main"}
            "paths" {"main" {"brain" "rule" "prompt" "You chat."}}}
   "tools" [{"id" "notify" "kind" "constant" "value" "sent"}]})

(defn- system [wf clock]
  (pipeline/system-from wf (assoc (pipeline/open-stores (spec/load-workflow wf)) :clock clock)))

(defn- restart
  "A fresh system over the same log and stores, the clock rebuilt from the log as a restarted runtime does."
  [sys wf]
  (let [elog (:log sys)
        recovered (wt/recovered-processing-time (map #(log/conversation-events elog %) (log/conversation-ids elog)))]
    (pipeline/system-from wf (assoc (select-keys sys [:store :state :log]) :clock (wt/manual-clock recovered)))))

(defn- turn
  ([cid tid] {:conversation-id cid :turn-id tid :user-id "u" :text "hello"})
  ([cid tid event-time] (assoc (turn cid tid) :metadata {"event_time_ms" (str event-time)})))

(defn- types [r] (mapv :type (:events r)))

(deftest manual-clock-never-moves-backwards
  (let [c (wt/manual-clock)
        step (rnd 1 5000)]
    (is (= 0 (wt/now-ms c)))
    (wt/advance! c step)
    (is (= step (wt/now-ms c)))
    (wt/advance! c 0)
    (is (= step (wt/now-ms c)))
    (is (thrown? clojure.lang.ExceptionInfo (wt/advance! c -1)))
    (is (thrown? clojure.lang.ExceptionInfo (wt/manual-clock -5)))))

(deftest processing-timer-is-scheduled-then-fires-once
  (let [after (rnd 100 100000)
        start (rnd 0 10000)
        payload {"channel" (str "ch-" (rand-int 1000))}
        clock (wt/manual-clock start)
        sys (system (workflow "processing" after payload) clock)
        opening (core/submit sys (turn "c1" "t1"))]
    (testing "the first turn schedules the timer after turn_received, due after_ms past the clock"
      (is (= [:turn-received :timer-scheduled :routed] (subvec (types opening) 0 3)))
      (is (= {:timer-id "followup" :clock "processing" :due-ms (+ start after)}
             (:payload (second (:events opening)))))
      (is (= start (get-in opening [:events 0 :payload :processing-time-ms]))))
    (wt/advance! clock (dec after))
    (testing "a turn before the deadline neither fires nor calls the tool"
      (let [early (core/submit sys (turn "c1" "t2"))]
        (is (empty? (:tool-calls early)))
        (is (not-any? #{:timer-fired} (types early)))
        (is (nil? (get-in early [:state :fired-timers])))))
    (wt/advance! clock (rnd 1 1000))
    (testing "the first turn at or past the deadline fires the timer before it is received"
      (let [fired (core/submit sys (turn "c1" "t3"))]
        (is (= [:timer-fired :tool-called :turn-received] (subvec (types fired) 0 3)))
        (is (= {:timer-id "followup" :due-ms (+ start after)} (:payload (first (:events fired)))))
        (is (= [{:tool "notify" :index 0 :attempt 1 :args payload :result "sent"}] (:tool-calls fired)))
        (is (= ["followup"] (get-in fired [:state :fired-timers])))))
    (wt/advance! clock after)
    (testing "a timer fires exactly once"
      (let [again (core/submit sys (turn "c1" "t4"))]
        (is (empty? (:tool-calls again)))
        (is (= ["followup"] (get-in again [:state :fired-timers])))
        (is (= 1 (count (filter #(= :timer-fired (:type %)) (core/events sys "c1")))))))
    (testing "another conversation gets its own timer against the current clock"
      (let [other (core/submit sys (turn "c2" "t1"))]
        (is (= (+ (wt/now-ms clock) after) (get-in other [:events 1 :payload :due-ms])))))))

(deftest event-timer-follows-the-watermark
  (let [t0 (rnd 1000 1000000)
        after (rnd 100 10000)
        clock (wt/manual-clock)
        sys (system (workflow "event" after {"channel" "email"}) clock)
        opening (core/submit sys (turn "c1" "t1" t0))
        ahead (+ t0 (rnd 1 after))
        advanced (core/submit sys (turn "c1" "t2" ahead))
        late (core/submit sys (turn "c1" "t3" (- t0 (rnd 1 t0))))]
    (is (= (+ t0 after) (get-in opening [:events 1 :payload :due-ms])) "due after_ms past the watermark")
    (is (= t0 (get-in opening [:state :watermark-ms])))
    (is (= ahead (get-in advanced [:state :watermark-ms])))
    (testing "a late turn is processed in arrival order and does not move the watermark back"
      (is (= :completed (:status late)))
      (is (= ahead (get-in late [:state :watermark-ms])))
      (is (empty? (:tool-calls late))))
    (wt/advance! clock (* 10 after))
    (is (empty? (:tool-calls (core/submit sys (turn "c1" "t4")))) "processing time does not fire an event timer")
    (let [fired (core/submit sys (turn "c1" "t5" (+ t0 after)))]
      (is (= :timer-fired (first (types fired))))
      (is (= ["followup"] (get-in fired [:state :fired-timers]))))))

(deftest due-timers-fire-in-due-ms-then-id-order
  (let [wf (assoc (workflow "processing" 500 {"k" "a"})
                  "timers" [{"id" "zeta" "after_ms" 300 "tool" "notify" "payload" {"k" "z"}}
                            {"id" "alpha" "after_ms" 300 "tool" "notify" "payload" {"k" "a"}}
                            {"id" "mid" "after_ms" 100 "tool" "notify" "payload" {"k" "m"}}])
        clock (wt/manual-clock)
        sys (system wf clock)]
    (core/submit sys (turn "c1" "t1"))
    (wt/advance! clock (rnd 300 5000))
    (let [fired (core/submit sys (turn "c1" "t2"))]
      (is (= ["mid" "alpha" "zeta"] (get-in fired [:state :fired-timers])))
      (is (= [{"k" "m"} {"k" "a"} {"k" "z"}] (mapv :args (:tool-calls fired))))
      (is (= [0 1 2] (mapv :index (:tool-calls fired)))))))

(deftest pending-timer-survives-restart-and-fires-once
  (let [after (rnd 200 100000)
        before (rnd 1 after)
        wf (workflow "processing" after {"channel" "email"})
        clock (wt/manual-clock)
        sys (system wf clock)]
    (core/submit sys (turn "c1" "t1"))
    (wt/advance! clock before)
    (is (empty? (:tool-calls (core/submit sys (turn "c1" "t2")))))
    (let [restarted (restart sys wf)]
      (testing "the clock is rebuilt from the log"
        (is (= before (wt/now-ms (:clock restarted)))))
      (testing "the pending timer is rebuilt from the log, not re-scheduled"
        (is (= {"followup" {:timer-id "followup" :clock "processing" :due-ms after}}
               (:pending (wt/fold (core/events restarted "c1"))))))
      (wt/advance! (:clock restarted) (- after before))
      (let [fired (core/submit restarted (turn "c1" "t3"))]
        (is (= [:timer-fired :tool-called :turn-received] (subvec (types fired) 0 3)))
        (is (= ["followup"] (get-in fired [:state :fired-timers])))
        (is (= 3 (get-in fired [:state :turn-count]))))
      (let [again (restart restarted wf)]
        (wt/advance! (:clock again) after)
        (let [r (core/submit again (turn "c1" "t4"))
              events (core/events again "c1")]
          (is (empty? (:tool-calls r)) "a fired timer never fires again after recovery")
          (is (= 1 (count (filter #(= :timer-scheduled (:type %)) events))) "never re-scheduled")
          (is (= 1 (count (filter #(= :timer-fired (:type %)) events))) "never double-fired")
          (is (log/dense? events)))))))

(deftest fold-derives-pending-from-scheduled-minus-fired
  (let [due (rnd 1 100000)
        events [{:type :turn-received :payload {:turn-id "t1" :text "x" :processing-time-ms 5 :event-time-ms 40}}
                {:type :timer-scheduled :payload {:timer-id "a" :clock "processing" :due-ms due}}
                {:type :timer-scheduled :payload {:timer-id "b" :clock "event" :due-ms (+ due 1)}}
                {:type :turn-received :payload {:turn-id "t2" :text "x" :processing-time-ms 9 :event-time-ms 30}}
                {:type :timer-fired :payload {:timer-id "a" :due-ms due}}
                {:type :timer-fired :payload {:timer-id "a" :due-ms due}}]
        state (wt/fold events)]
    (is (= #{"b"} (set (keys (:pending state)))))
    (is (= ["a"] (:fired state)) "a duplicate timer_fired is ignored")
    (is (= 40 (:watermark-ms state)) "a lower event time does not move the watermark back")
    (is (= 9 (:processing-time-ms state)))
    (is (= 9 (wt/recovered-processing-time [events []])))
    (is (= 0 (wt/recovered-processing-time [])))
    (is (= {:turn-count 2 :transcript-length 0 :watermark-ms 40 :fired-timers ["a" "a"]}
           (log/reduce-state events)))))

(deftest workflows-without-timers-record-no-clock
  (let [wf (dissoc (workflow "processing" 100 {}) "timers")
        sys (system wf (wt/manual-clock (rnd 1 1000)))
        r (core/submit sys (turn "c1" "t1"))]
    (is (= {:turn-id "t1" :text "hello"} (get-in r [:events 0 :payload])))
    (is (nil? (get-in r [:state :watermark-ms])))
    (is (= :completed (:status r)))))
