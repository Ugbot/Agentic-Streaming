(ns agentic.spi-test
  "Parity hygiene: the Embedder + Classifier SPIs wrap the existing implementations
   without changing behaviour (delegation parity)."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.embedder :as embedder]
            [agentic.classifier :as classifier]
            [agentic.retrieval :as retrieval]
            [agentic.guardrail :as guardrail]))

(deftest embedder-spi
  (let [e (embedder/hashing-embedder 256)]
    (testing "embed-text returns a 256-vector"
      (let [v (embedder/embed-text e "hello world")]
        (is (vector? v))
        (is (= 256 (count v)))
        (is (every? double? v))))
    (testing "embed-text delegates to agentic.retrieval/embed (parity)"
      (is (= (retrieval/embed "hello world" 256)
             (embedder/embed-text e "hello world"))))
    (testing "embed-batch returns one vector per input text"
      (let [vs (embedder/embed-batch e ["a" "b"])]
        (is (= 2 (count vs)))
        (is (every? #(= 256 (count %)) vs))
        (is (= [(retrieval/embed "a" 256) (retrieval/embed "b" 256)] vs))))
    (testing "default dim is 256"
      (is (= 256 (count (embedder/embed-text (embedder/hashing-embedder) "x")))))))

(deftest classifier-spi
  (let [lexicon {"toxic" ["idiot" "hate"] "ok" ["please" "thanks"]}
        c (classifier/lexicon-classifier lexicon "ok")]
    (testing "classify returns {:label :score :scores}"
      (let [r (classifier/classify c "you idiot")]
        (is (= "toxic" (:label r)))
        (is (number? (:score r)))
        (is (map? (:scores r)))))
    (testing "non-toxic text classifies as ok"
      (is (= "ok" (:label (classifier/classify c "please help")))))
    (testing "delegates to agentic.guardrail/lexicon-classify (parity)"
      (is (= (guardrail/lexicon-classify lexicon "ok" "you idiot")
             (classifier/classify c "you idiot"))))))
