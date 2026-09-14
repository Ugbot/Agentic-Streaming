(ns agentic.context-window-test
  "`context.compaction: window`: the fold retains the most recent `max_items` messages in log order,
   `transcript_length` reports what is retained, the log and `turn_count` are untouched, `none`
   retains everything, and conversations are windowed independently."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.pipeline :as pipeline]
            [agentic.core :as core]
            [agentic.log :as log]))

(defn- rand-between [lo hi] (+ lo (rand-int (inc (- hi lo)))))

(defn- completed-turn
  "The events of one completed turn: turn_received, memory_written (two messages), turn_completed."
  [i]
  [{:turn-id (str "t" i) :type :turn-received :payload {:turn-id (str "t" i) :text (str "u" i)}}
   {:turn-id (str "t" i) :type :memory-written
    :payload {:messages [{:role "user" :text (str "u" i)} {:role "assistant" :text (str "a" i)}]}}
   {:turn-id (str "t" i) :type :turn-completed :payload {:reply (str "a" i)}}])

(defn- log-of [turns]
  (vec (map-indexed (fn [i e] (assoc e :sequence i)) (mapcat completed-turn (range turns)))))

(deftest window-bounds-transcript-length-not-turn-count
  (dotimes [_ 20]
    (let [max-items (rand-between 1 8)
          turns (rand-between 1 11)
          written (* 2 turns)
          events (log-of turns)
          window {:max-items max-items :compaction "window"}
          full (log/reduce-state events)
          bounded (log/reduce-state events window)]
      (is (= written (:transcript-length full)) "no context keeps every message")
      (is (= (min written max-items) (:transcript-length bounded)))
      (is (= turns (:turn-count full) (:turn-count bounded)) "turn_count is unaffected by compaction")
      (is (= (* 3 turns) (count events)) "the log itself is never compacted")
      (is (= full (log/reduce-state events {:max-items 1 :compaction "none"})) "none retains everything")
      (is (= full (log/reduce-state events {:max-tokens 8 :compaction "moscow"})) "moscow is not a message window"))))

(deftest retain-window-keeps-the-most-recent-messages-in-order
  (dotimes [_ 20]
    (let [max-items (rand-between 1 6)
          n (rand-between max-items (+ max-items 10))
          messages (mapv (fn [i] {:role (if (even? i) "user" "assistant") :content (str "m" i)}) (range n))
          kept (log/retain-window messages {:max-items max-items :compaction "window"})]
      (is (= (subvec messages (- n max-items)) kept))
      (is (= messages (log/retain-window messages nil)))
      (is (= messages (log/retain-window messages {:max-items 1 :compaction "none"})))))
  (is (thrown? clojure.lang.ExceptionInfo (log/window-size {:compaction "window"})))
  (is (thrown? clojure.lang.ExceptionInfo (log/window-size {:compaction "window" :max-items 0})))
  (is (nil? (log/window-size nil)))
  (is (nil? (log/window-size {:max-items 3}))))

(defn- windowed-workflow [max-items]
  {"spec_version" "agentic/v1" "backend" "local"
   "context" {"max_items" max-items "compaction" "window"}
   "agent" {"id" "chat"
            "router" {"kind" "keyword" "default" "main"}
            "paths" {"main" {"brain" "rule" "prompt" "You chat."}}}})

(deftest running-workflow-reports-the-retained-window
  (let [max-items (rand-between 1 6)
        turns (rand-between 1 7)
        sys (pipeline/system-from (windowed-workflow max-items) {})
        results (mapv #(core/submit sys {:conversation-id "c1" :turn-id (str "t" %) :user-id "u" :text (str "hello " %)})
                      (range 1 (inc turns)))
        other (core/submit sys {:conversation-id "c2" :turn-id "x1" :user-id "u" :text "other conversation"})]
    (testing "every turn completes and reports the retained transcript"
      (is (every? #(= :completed (:status %)) results))
      (is (= (range 1 (inc turns)) (map #(get-in % [:state :turn-count]) results)))
      (is (= (map #(min (* 2 %) max-items) (range 1 (inc turns)))
             (map #(get-in % [:state :transcript-length]) results))))
    (testing "the log keeps every message and the folded state reads the window"
      (is (= (* 2 turns) (:transcript-length (log/reduce-state (core/events sys "c1")))))
      (is (= {:turn-count turns :transcript-length (min (* 2 turns) max-items)} (core/state sys "c1"))))
    (testing "conversations are windowed independently"
      (is (= {:turn-count 1 :transcript-length (min 2 max-items)} (:state other))))))
