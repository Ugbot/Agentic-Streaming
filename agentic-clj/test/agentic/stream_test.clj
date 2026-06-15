(ns agentic.stream-test
  "Phase 1: the stream substrate. Driving a channel of events must be identical to N× submit, and
   observers must see every event — the seam later phases (CEP/windows/timers) build on. Mirrors
   jagentic-core's StreamRuntimeTest goldens."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [agentic.core :as core]
            [agentic.event :as ev]
            [agentic.stream :as stream]))

(defn- turns []
  [(ev/event "c1" "alice" "what is my balance?")
   (ev/event "c2" "bob" "tell me about crypto cash-back")
   (ev/event "c1" "alice" "hello there")])

(deftest streaming-equals-repeated-submit
  (testing "driving a seed-channel through a stream-runtime matches N× submit, in order"
    ;; Reference: a FRESH banking-system, submit each event individually.
    (let [direct (core/banking-system)
          via-submit (mapv #(:path (core/submit direct %)) (turns))
          ;; Streaming form: a seed-channel through a stream-runtime over a FRESH banking-system.
          sr (stream/stream-runtime (core/banking-system))
          results (stream/run sr (stream/seed-channel (turns)))
          via-stream (mapv :path results)]
      (is (= via-submit via-stream) "streaming must match repeated submit, in order")
      (is (= ["payments" "cards" "general"] via-stream)))))

(deftest observers-see-every-event-before-the-turn
  (testing "observers see every event in arrival order"
    (let [seen (atom [])
          sr (-> (stream/stream-runtime (core/banking-system))
                 (stream/observe (fn [e] (swap! seen conj (:conversation-id e)))))]
      (stream/run sr (stream/seed-channel (turns)))
      (is (= ["c1" "c2" "c1"] @seen)))))

(deftest queue-channel-drains-in-fifo-order
  (testing "queue-channel offers drain FIFO into turns"
    (let [q (-> (stream/queue-channel)
                (stream/offer (ev/event "c1" "u" "what is my balance?"))
                (stream/offer (ev/event "c1" "u" "tell me about crypto cash-back")))
          results (stream/run (stream/stream-runtime (core/banking-system)) q)]
      (is (= 2 (count results)))
      (is (= "payments" (:path (first results))))
      (is (= "cards" (:path (second results))))
      (is (str/includes? (:reply (first results)) "1234.56")))))
