(ns agentic.windows-test
  "Phase 3: portable keyed time-windows. Sliding (the VelocityDetector), tumbling, and session
   aggregates. Mirrors jagentic-core's org.jagentic.core.windows.* goldens — behaviour at parity."
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.windows :as w]))

(deftest sliding-count
  (testing "count within (ts-window, ts]; independent per key; old events evicted"
    (let [sw (w/sliding-window 60000)]
      (doseq [ts [1000 2000 3000 4000]]
        (w/slide-add sw "h1" ts))
      (is (= 5 (:count (w/slide-add sw "h1" 5000))) "all 5 events inside the 60s window")
      (is (= 1 (:count (w/slide-add sw "h1" 200000))) "only the latest survives the eviction")
      (is (= 1 (:count (w/slide-add sw "h2" 1000))) "a fresh key is independent"))))

(deftest sliding-sum
  (testing "sum of values within the window"
    (let [sw (w/sliding-window 10000)]
      (w/slide-add sw "acct" 0 100.0)
      (let [agg (w/slide-add sw "acct" 5000 250.0)]
        (is (= 2 (:count agg)))
        (is (= 350.0 (:sum agg)))))))

(deftest tumbling-buckets
  (testing "events accumulate into floor(ts/window) buckets; later-bucket events emit the prior"
    (let [tw (w/tumbling-window 1000)]
      (is (nil? (w/tumble-add tw "k" 100)) "first event opens bucket 0, nothing closed")
      (is (nil? (w/tumble-add tw "k" 200)) "still bucket 0")
      (let [closed (w/tumble-add tw "k" 1500)]
        (is (= 0 (:start closed)) "bucket 0 starts at 0")
        (is (= 2 (:count closed)) "two events in bucket 0"))
      (let [closed (w/tumble-close tw "k")]
        (is (= 1000 (:start closed)) "open bucket 1 starts at 1000")
        (is (= 1 (:count closed)) "one event in bucket 1")))))

(deftest session-grouping
  (testing "events within gap join a session; a larger gap closes (emits) the prior session"
    (let [sw (w/session-window 5000)]
      (is (nil? (w/session-add sw "u" 0)) "first event opens a session")
      (is (nil? (w/session-add sw "u" 2000)) "2000-0 <= gap, same session")
      (let [closed (w/session-add sw "u" 10000)]
        (is (= 0 (:start closed)))
        (is (= 2000 (:end closed)))
        (is (= 2 (:count closed)) "the prior session held two events"))
      (is (= 1 (:count (w/session-close sw "u"))) "the new session holds the 10000 event")
      (is (nil? (w/session-close sw "u")) "nothing open after close"))))
