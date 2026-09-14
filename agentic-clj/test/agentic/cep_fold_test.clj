(ns agentic.cep-fold-test
  "agentic.cep-fold: the in-turn sequence pattern fold against a naive reference matcher on random
   turn sequences, plus its wiring into a running system (tool_called on the completing turn)."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [agentic.cep-fold :as cf]
            [agentic.core :as core]
            [agentic.log :as log]
            [agentic.pipeline :as pipeline]))

(def ^:private words ["anomaly" "heartbeat" "ok" "disk" "cpu" "noise"])

(defn- rid [prefix] (str prefix "-" (Long/toHexString (.nextLong (java.util.concurrent.ThreadLocalRandom/current)))))

(defn- random-text [^java.util.Random rng]
  (let [w (nth words (.nextInt rng (count words)))
        w (case (.nextInt rng 3) 0 (str/upper-case w) 1 (str/capitalize w) w)]
    (case (.nextInt rng 3)
      0 w
      1 (str w ": " (.nextInt rng 100))
      (str "note " w " seen"))))

(defn- random-turns [^java.util.Random rng n]
  (loop [i 0 ts 0 out []]
    (if (= i n)
      out
      (let [ts (+ ts (.nextInt rng 90000))]
        (recur (inc i) ts (conj out {:turn-id (str "t" i) :text (random-text rng)
                                     :metadata {"event_time_ms" (str ts)}}))))))

(defn- random-rule [^java.util.Random rng timed?]
  (let [stages (vec (for [i (range (inc (.nextInt rng 4)))]
                      (cond-> {:stage (str "s" i)
                               :where (if (zero? (.nextInt rng 5)) nil {:text-contains (nth words (.nextInt rng 3))})}
                        (and (pos? i) (.nextBoolean rng)) (assoc :contiguity "followedBy"))))]
    (cond-> {:name "p" :key "conversation_id" :pattern stages :on-match {:kind "tool" :tool "open_ticket"}}
      timed? (assoc :ts "metadata.event_time_ms" :within (* 30000 (inc (.nextInt rng 8)))))))

(defn- naive-completed
  "Left-to-right scan with an explicit search per attempt: start at the first turn matching stage
   one, walk the remaining stages; a turn outside the window ends the attempt and is where the scan
   resumes; a turn breaking a `next` stage ends the attempt and is consumed; a completed match is
   consumed whole."
  [pattern turns]
  (let [stages (:stages pattern) n (count turns)]
    (loop [i 0 done []]
      (cond
        (>= i n) done
        (not (cf/stage-matches? (first stages) (:text (nth turns i)))) (recur (inc i) done)
        (= 1 (count stages)) (recur (inc i) (conj done i))
        :else
        (let [start-ts (if (:ts-key pattern) (cf/timestamp pattern (nth turns i)) 0)
              [end resume] (loop [j (inc i) stage 1]
                             (cond
                               (>= j n) [nil n]
                               (and (:within-ms pattern) (> (- (cf/timestamp pattern (nth turns j)) start-ts) (:within-ms pattern))) [nil j]
                               (cf/stage-matches? (nth stages stage) (:text (nth turns j)))
                               (if (= (inc stage) (count stages)) [j nil] (recur (inc j) (inc stage)))
                               (= :next (:contiguity (nth stages stage))) [nil (inc j)]
                               :else (recur (inc j) stage)))]
          (if end
            (recur (inc end) (conj done end))
            (recur resume done)))))))

(deftest fold-agrees-with-naive-matcher-on-random-sequences
  (let [rng (java.util.Random.)]
    (dotimes [_ 400]
      (let [pattern (cf/compile-pattern (random-rule rng (< (.nextDouble rng) 0.7)))
            turns (random-turns rng (.nextInt rng 13))
            expected (set (naive-completed pattern turns))]
        (is (= (vec (sort expected)) (cf/completed-indexes pattern turns)) (pr-str pattern (mapv :text turns)))
        (doseq [n (range (inc (count turns)))]
          (is (= (contains? expected (dec n)) (cf/completes-on? pattern (subvec turns 0 n)))
              (pr-str pattern (mapv :text (subvec turns 0 n)))))))))

(deftest matching-is-case-insensitive-and-consumes-matches
  (let [pattern (cf/compile-pattern {:name "p" :pattern [{:stage "a" :where {:text-contains "Anomaly"}}
                                                        {:stage "b" :where {:text-contains "anomaly"} :contiguity "followedBy"}]
                                     :on-match {:kind "tool" :tool "t"}})
        turns (mapv (fn [i text] {:turn-id (str i) :text text :metadata {}})
                    (range) ["ANOMALY one" "fine" "anomaly: two" "anomaly three" "Anomaly four" "anomaly five"])]
    (is (= [2 4] (cf/completed-indexes pattern turns)))
    (is (= [false false true false true false] (mapv #(cf/completes-on? pattern (subvec turns 0 (inc %))) (range 6))))
    (is (false? (cf/completes-on? pattern [])))))

(deftest window-is-event-time-between-first-and-last-stage
  (let [rng (java.util.Random.)
        within (+ 1000 (.nextInt rng 100000))
        pattern (cf/compile-pattern {:name "p" :ts "metadata.event_time_ms" :within within
                                     :pattern [{:stage "a" :where {:text-contains "x"}}
                                               {:stage "b" :where {:text-contains "x"} :contiguity "followedBy"}]
                                     :on-match {:kind "tool" :tool "t"}})
        turn (fn [id ts] {:turn-id id :text "x" :metadata {"event_time_ms" (str ts)}})]
    (testing "exactly `within` apart still matches"
      (is (cf/completes-on? pattern [(turn "a" 0) (turn "b" within)])))
    (testing "past the window the late turn restarts the partial"
      (is (not (cf/completes-on? pattern [(turn "a" 0) (turn "b" (inc within))])))
      (is (cf/completes-on? pattern [(turn "a" 0) (turn "b" (inc within)) (turn "c" (+ within 2))])))
    (testing "a turn without the ts field is a validation error"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"lacks metadata.event_time_ms"
                            (cf/completes-on? pattern [(turn "a" 0) {:turn-id "b" :text "x" :metadata {}}]))))))

(deftest compile-separates-tool-rules-from-the-rest
  (let [rules [{:name "t" :pattern [{:stage "a"}] :on-match {:kind "tool" :tool "open_ticket"}}
               {:name "s" :pattern [{:stage "a"}] :on-match {:kind "submit" :text "hi"}}
               {:name "d" :pattern [{:stage "a"}]}]]
    (is (= ["t"] (mapv :name (cf/compile-patterns rules))))
    (is (= ["s" "d"] (mapv :name (cf/without-tool-actions rules))))
    (is (= [:next] (mapv :contiguity (:stages (first (cf/compile-patterns rules))))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (cf/compile-pattern {:name "k" :key "metadata.host" :pattern [{:stage "a"}] :on-match {:kind "tool" :tool "t"}})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (cf/compile-pattern {:name "w" :within 5 :pattern [{:stage "a"}] :on-match {:kind "tool" :tool "t"}})))))

(defn- workflow [within]
  {"spec_version" "agentic/v1" "backend" "local"
   "agent" {"id" "monitor" "router" {"kind" "keyword" "default" "monitor"}
            "paths" {"monitor" {"brain" "rule" "prompt" "You acknowledge signals."}}}
   "tools" [{"id" "open_ticket" "kind" "constant" "value" "TICKET-OPENED"}]
   "cep" [{"name" "host_incident" "key" "conversation_id" "ts" "metadata.event_time_ms" "within" within
           "pattern" [{"stage" "first" "where" {"text_contains" "anomaly"}}
                      {"stage" "second" "where" {"text_contains" "anomaly"} "contiguity" "followedBy"}
                      {"stage" "third" "where" {"text_contains" "anomaly"} "contiguity" "followedBy"}]
           "on_match" {"kind" "tool" "tool" "open_ticket"}}]})

(defn- turn [cid id text ts]
  {:conversation-id cid :turn-id id :user-id "u" :text text :metadata {"event_time_ms" (str ts)}})

(deftest completing-turn-records-the-tool-call
  (let [rng (java.util.Random.)
        step (+ 1000 (.nextInt rng 59000))
        wf (workflow 300000)
        system (pipeline/system-from wf (pipeline/open-stores wf))
        host (rid "host") other (rid "host")
        r1 (core/submit system (turn host "a1" "Anomaly: cpu" 0))
        r2 (core/submit system (turn host "a2" "heartbeat ok" step))
        rb (core/submit system (turn other "b1" "anomaly: disk" step))
        r3 (core/submit system (turn host "a3" "anomaly: io" (* 2 step)))
        r4 (core/submit system (turn host "a4" "anomaly: again" (* 3 step)))
        r5 (core/submit system (turn host "a5" "anomaly: consumed" (* 4 step)))]
    (is (= [[] [] [] [] []] (mapv :tool-calls [r1 r2 rb r3 r5])))
    (is (= [{:tool "open_ticket" :index 0 :attempt 1 :args {"pattern" "host_incident" "key" host} :result "TICKET-OPENED"}]
           (:tool-calls r4)))
    (is (= :completed (:status r4)))
    (let [kinds (mapv :type (:events r4))]
      (is (= [:turn-received :routed :tool-called :brain-started] (subvec kinds 0 4)))
      (is (= :turn-completed (peek kinds))))
    (testing "the turns fold back out of the log and replay the same decision"
      (let [events (core/events system host)
            pattern (first (cf/compile-patterns (:cep (:workflow system))))
            turns (cf/turns-of events)]
        (is (= ["a1" "a2" "a3" "a4" "a5"] (mapv :turn-id turns)))
        (is (= [0 step (* 2 step) (* 3 step) (* 4 step)]
               (mapv #(get-in % [:payload :event-time-ms]) (filter #(= :turn-received (:type %)) events))))
        (is (= [false false false true false] (mapv #(cf/completes-on? pattern (subvec turns 0 %)) (range 1 6))))))
    (testing "the tool failing fails the completing turn as a tool error"
      (let [wf (assoc wf "tools" [{"id" "open_ticket" "kind" "failing" "value" "x"}])
            system (pipeline/system-from wf (pipeline/open-stores wf))
            host (rid "host")]
        (core/submit system (turn host "a1" "anomaly" 0))
        (core/submit system (turn host "a2" "anomaly" 1))
        (let [r (core/submit system (turn host "a3" "anomaly" 2))]
          (is (= :failed (:status r)))
          (is (= :tool (get-in r [:error :class])))
          (is (= [:turn-received :routed :tool-failed :turn-failed] (mapv :type (:events r)))))))
    (testing "state is a fold over the log the turns were read from"
      (is (= 5 (:turn-count (log/reduce-state (core/events system host))))))))
