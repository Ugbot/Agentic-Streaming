(ns agentic.replay-test
  "Phase 5: replay / time-travel. Recording an event-log off the stream observer seam must capture every
   inbound event; replaying it through a FRESH banking-system must reproduce the routed outcomes
   exactly; replay-until must stop early at the as-of point. Mirrors jagentic-core's ReplayerTest /
   EventLogTest goldens."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.core :as core]
            [agentic.event :as ev]
            [agentic.stream :as stream]
            [agentic.replay :as replay]))

(defn- turns []
  [(ev/event "c1" "alice" "what is my balance?")
   (ev/event "c2" "bob" "tell me about crypto cash-back")
   (ev/event "c1" "alice" "hello there")])

(deftest recording-via-stream-observer-captures-every-event
  (testing "an event-log observed off the stream seam captures every event, keyed by conversation"
    (let [log (replay/event-log)
          sr (-> (stream/stream-runtime (core/banking-system))
                 (stream/observe (fn [e] (replay/record log e))))]
      (stream/run sr (stream/seed-channel (turns)))
      (is (= (turns) (replay/events log)) "log captures every event, in arrival order")
      (is (= 2 (count (replay/events-for log "c1"))) "two events for c1")
      (is (= 1 (count (replay/events-for log "c2"))) "one event for c2")
      (is (= [] (replay/events-for log "nope")) "unknown conversation is empty"))))

(deftest replay-reproduces-the-routed-outcomes
  (testing "replaying the recorded log through a FRESH banking-system reproduces the routed paths"
    (let [log (replay/event-log)
          sr (-> (stream/stream-runtime (core/banking-system))
                 (stream/observe (fn [e] (replay/record log e))))]
      (stream/run sr (stream/seed-channel (turns)))
      (let [results (replay/replay (replay/events log) (core/banking-system))]
        (is (= 3 (count results)))
        (is (= ["payments" "cards" "general"] (mapv :path results)))))))

(deftest replay-until-stops-early-at-the-as-of-point
  (testing "replay-until replays only the first n events — state as-of that point"
    (let [log (replay/event-log)
          sr (-> (stream/stream-runtime (core/banking-system))
                 (stream/observe (fn [e] (replay/record log e))))]
      (stream/run sr (stream/seed-channel (turns)))
      (let [evs (replay/events log)]
        (let [results (replay/replay-until evs 2 (core/banking-system))]
          (is (= 2 (count results)))
          (is (= ["payments" "cards"] (mapv :path results))))
        (testing "n is clamped to [0 (count events)]"
          (is (= 0 (count (replay/replay-until evs 0 (core/banking-system)))))
          (is (= 0 (count (replay/replay-until evs -5 (core/banking-system)))))
          (is (= 3 (count (replay/replay-until evs 99 (core/banking-system))))))))))
