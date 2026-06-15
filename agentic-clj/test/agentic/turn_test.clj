(ns agentic.turn-test
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.core :as core]
            [agentic.event :as ev]
            [agentic.retrieval :as r]
            [agentic.banking :as banking]
            [agentic.store :as store]))

(deftest banking-routing-and-reply-goldens
  (let [sys (core/banking-system)]
    (testing "balance → payments path → get_balance tool"
      (let [res (core/submit sys (ev/event "c1" "u" "what is my balance?"))]
        (is (= "payments" (:path res)))
        (is (= "[payments] Your balance is 1234.56." (:reply res)))
        (is (true? (:ok res)))
        (is (= ["get_balance"] (:tool-calls res)))))
    (testing "card → cards path (fallback reply)"
      (is (= "cards" (:path (core/submit sys (ev/event "c2" "u" "what card types do you offer?"))))))
    (testing "crypto cash-back → cards path → retrieval hit"
      (let [res (core/submit sys (ev/event "c3" "u" "tell me about crypto cash-back"))]
        (is (= "cards" (:path res)))
        (is (re-find #"Crypto cash-back" (:reply res)))))
    (testing "no keyword → general"
      (is (= "general" (:path (core/submit sys (ev/event "c4" "u" "hello there"))))))))

(deftest multi-turn-persists-transcript
  (let [sys (core/banking-system)]
    (core/submit sys (ev/event "c1" "u" "what card types do you offer?"))
    (core/submit sys (ev/event "c1" "u" "tell me about crypto cash-back"))
    ;; 2 turns × (user + assistant) = 4 messages
    (is (= 4 (store/message-count (:store sys) "c1")))))

(deftest fnv-embedder-parity
  (testing "FNV-1a offset basis (empty string) matches the cross-core constant"
    (is (= 2166136261 (r/fnv1a-32 ""))))
  (testing "deterministic + non-negative unsigned"
    (is (= (r/fnv1a-32 "balance") (r/fnv1a-32 "balance")))
    (is (<= 0 (r/fnv1a-32 "balance") 0xFFFFFFFF)))
  (testing "embedder is L2-normalized and dim-sized"
    (let [v (r/embed "crypto cash-back" banking/dim)]
      (is (= banking/dim (count v)))
      (is (< (Math/abs (- 1.0 (Math/sqrt (reduce + (map #(* % %) v))))) 1e-9)))))
