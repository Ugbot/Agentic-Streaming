(ns agentic.parallel-test
  "The `parallelism` primitive on the Clojure runtime: N conversation mailboxes with M turns each,
   delivered in one shuffled interleaving without waiting. A barrier tool that only trips when every
   conversation is inside its first turn together proves that different conversations run at the
   same time; per-conversation event sequences, state and tool arguments prove that each mailbox
   stays a serial writer whose state, transcript and tool calls never leak to another."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [agentic.spec :as spec]
            [agentic.pipeline :as pipeline]
            [agentic.tools :as tools]
            [agentic.core :as core]
            [agentic.log :as log])
  (:import [java.util UUID]
           [java.util.concurrent CyclicBarrier TimeUnit]))

(def ^:private meet "meet")
(def ^:private probe "probe")

(def ^:private workflow
  {"spec_version" "agentic/v1" "backend" "local"
   "agent" {"id" "parallel"
            "router" {"kind" "keyword" "default" "main" "rules" {"main" ["charge"]}}
            "paths" {"main" {"brain" "rule" "prompt" "p"
                             "tool_triggers" {"rendezvous" meet "charge" probe}}}
            "verifier" {"kind" "none"}}
   "tools" [{"id" meet "kind" "constant" "value" "m"}
            {"id" probe "kind" "constant" "value" "p"}]})

(defn- rnd [] (subs (str (UUID/randomUUID)) 0 8))

(defn- plan-for
  "M turns of one conversation: the first meets at the barrier, the rest randomly probe or chat."
  [cid m]
  (into [{:cid cid :tid (str cid "-t0") :text (str "rendezvous from " cid) :tool meet}]
        (for [i (range 1 m)
              :let [probes? (rand-nth [true false])]]
          {:cid cid :tid (str cid "-t" i)
           :text (str (if probes? "charge " "hello ") i " from " cid)
           :tool (when probes? probe)})))

(defn- interleave-randomly
  "One random interleaving of the per-conversation plans that keeps each conversation's own order."
  [plans]
  (loop [queues (vec (remove empty? plans)) out []]
    (if (empty? queues)
      out
      (let [i (rand-int (count queues))
            q (queues i)
            queues' (if (next q) (assoc queues i (subvec q 1)) (into (subvec queues 0 i) (subvec queues (inc i))))]
        (recur queues' (conj out (first q)))))))

(defn- ->event [{:keys [cid tid text]}]
  ;; The user id doubles as the conversation id so tool args reveal which conversation called.
  {:conversation-id cid :turn-id tid :user-id cid :text text})

(deftest conversations-run-concurrently-while-each-mailbox-stays-a-serial-isolated-writer
  (let [n (+ 3 (rand-int 5))
        m (+ 3 (rand-int 5))
        cids (mapv #(str "c" % "-" (rnd)) (range n))
        plans (mapv #(plan-for % m) cids)
        interleaved (interleave-randomly plans)
        barrier (CyclicBarrier. n)
        probes-by-user (atom {})
        sys (pipeline/system-from workflow (pipeline/open-stores (spec/load-workflow workflow)))]
    (tools/register (:tools sys) meet "barrier"
                    (fn [args]
                      (.await barrier 20 TimeUnit/SECONDS)
                      (str "met:" (get args "user"))))
    (tools/register (:tools sys) probe "per-user counter"
                    (fn [args]
                      (Thread/sleep (rand-int 3))
                      (let [user (get args "user")
                            counts (swap! probes-by-user update user (fnil inc 0))]
                        (str "probe:" user ":" (get counts user)))))
    (let [outcomes (mapv (fn [p] [p (core/submit-async sys (->event p))]) interleaved)
          results (into {} (map (fn [[p o]]
                                  (let [r (deref o 60000 ::timeout)]
                                    (when (instance? Throwable r) (throw r))
                                    [p r]))
                                outcomes))]
      (testing "every conversation reached the barrier inside its first turn at the same time"
        (is (not (.isBroken barrier)))
        (is (every? #(not= ::timeout %) (vals results))))
      (testing "each result belongs to the turn that was submitted and carries only its own tool calls"
        (doseq [[p r] results]
          (is (= :completed (:status r)) (str (:tid p) ": " (:error r)))
          (is (= (:cid p) (:conversation-id r)))
          (is (= (:tid p) (:turn-id r)))
          (is (= (if (:tool p) 1 0) (count (:tool-calls r))) (:tid p))
          (doseq [call (:tool-calls r)]
            (is (= (:tool p) (:tool call)))
            (is (= (:cid p) (get-in call [:args "user"])) "tool args belong to the calling conversation")
            (is (str/includes? (str (:result call)) (:cid p))))))
      (testing "each conversation's log is one dense sequence in submission order, never interleaved"
        (doseq [plan plans
                :let [cid (:cid (first plan))
                      events (core/events sys cid)]]
          (is (log/dense? events) (str cid " has one gapless sequence across its turns"))
          (is (= (mapv :tid plan)
                 (mapv :turn-id (filter #(= :turn-received (:type %)) events)))
              (str cid " processes its turns in the order they were submitted"))
          (is (= (mapv :tid plan) (vec (distinct (map :turn-id events))))
              (str "events of one turn never interleave within " cid))
          (doseq [[i p] (map-indexed vector plan)
                  :let [r (results p)]]
            (is (= (inc i) (get-in r [:state :turn-count])) (str "turn_count counts only " cid))
            (is (= (* 2 (inc i)) (get-in r [:state :transcript-length]))
                (str "transcript holds only " cid "'s messages")))
          (is (= (count (filter #(= probe (:tool %)) plan))
                 (get @probes-by-user cid 0))
              (str "probe calls attributed to " cid))))
      (testing "no probe ran twice or went missing"
        (is (= (count (filter #(= probe (:tool %)) interleaved))
               (reduce + (vals @probes-by-user))))))))
