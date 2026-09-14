(ns agentic.conformance
  "Binding of the shared v1 conformance fixtures (spec/conformance/v1/fixtures/*.yaml, read in place,
   never copied) to the Clojure runtime. A fixture is built with agentic.pipeline, its turns are
   delivered in order — honouring restart_runtime, signal and concurrent_with — and each normalized
   result is compared with the rules of spec/conformance/v1/README.md. A fixture whose `requires` we
   do not support is a skip, never a pass."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [agentic.spec :as spec]
            [agentic.pipeline :as pipeline]
            [agentic.core :as core]
            [agentic.log :as log]))

(def capabilities
  "What this runtime claims, in the terms of spec/v1/primitives.md."
  {"routing" :supported "rule_brain" :supported "tools" :supported
   "structured_tool_args" :supported "guardrails" :supported "verifier" :supported
   "ordering" :supported "idempotency" :supported "retry" :supported "memory" :supported
   "retrieval" :supported "replay" :supported "suspend_resume" :supported "saga" :supported
   "a2a" :supported "durable_store" :supported "context_window" :supported "llm_brain" :supported})

(defn fixtures-dir []
  (io/file (spec/spec-root) "conformance" "v1" "fixtures"))

(defn fixture-files []
  (->> (.listFiles (fixtures-dir))
       (filter #(re-find #"\.ya?ml$" (.getName ^java.io.File %)))
       (sort-by #(.getName ^java.io.File %))))

(defn read-fixture [^java.io.File f]
  (spec/read-document (.getPath f)))

(defn- workflow-of [fixture ^java.io.File f]
  (or (get fixture "workflow")
      (spec/read-document (.getPath (io/file (.getParentFile f) (get fixture "workflow_ref"))))))

(defn unsupported
  "The fixture's required capabilities this runtime does not claim as `supported`."
  [fixture]
  (vec (remove #(= :supported (get capabilities %)) (get fixture "requires"))))

;; ---- comparison rules ----

(defn- mismatch [field want got] (str field ": expected " (pr-str want) ", got " (pr-str got)))

(defn check-expectation
  "Failure messages for one expectation (fixture YAML map) against one wire-form result."
  [expected actual]
  (let [problems (atom [])
        fail! (fn [& args] (swap! problems conj (apply mismatch args)))]
    (doseq [field ["conversation_id" "status" "path" "reply"]]
      (when (and (contains? expected field) (not= (get expected field) (get actual field)))
        (fail! field (get expected field) (get actual field))))
    (when-let [re (get expected "reply_matches")]
      (let [reply (or (get actual "reply") "")]
        (when-not (re-find (re-pattern re) reply) (fail! "reply_matches" re reply))))
    (when (contains? expected "error_class")
      (let [got (get-in actual ["error" "class"])]
        (when (not= got (get expected "error_class")) (fail! "error_class" (get expected "error_class") got))))
    (when-let [want (get expected "tool_calls")]
      (let [got (get actual "tool_calls" [])]
        (if (not= (count want) (count got))
          (fail! "tool_calls length" (count want) (count got))
          (doseq [[i w g] (map vector (range) want got)]
            (when (not= (get w "tool") (get g "tool")) (fail! (str "tool_calls[" i "].tool") (get w "tool") (get g "tool")))
            (doseq [k ["index" "attempt" "args"]]
              (when (and (contains? w k) (not= (get w k) (get g k))) (fail! (str "tool_calls[" i "]." k) (get w k) (get g k))))
            (when (not= (boolean (get w "failed" false)) (some? (get g "error")))
              (fail! (str "tool_calls[" i "].failed") (get w "failed" false) (get g "error")))))))
    (let [types (mapv #(get % "type") (get actual "events" []))]
      (loop [remaining types [wanted & more] (get expected "events_include")]
        (when wanted
          (let [i (.indexOf ^java.util.List remaining wanted)]
            (if (neg? i)
              (do (swap! problems conj (str "events_include: " wanted " missing or out of order in " (pr-str types)))
                  (recur remaining more))
              (recur (subvec remaining (inc i)) more)))))
      (doseq [unwanted (get expected "events_exclude")]
        (when (some #{unwanted} types)
          (swap! problems conj (str "events_exclude: " unwanted " present in " (pr-str types))))))
    (doseq [[k want] (get expected "state_includes")]
      (let [got (get-in actual ["state" k])]
        (when (not= want got) (fail! (str "state." k) want got))))
    @problems))

;; ---- delivery ----

(defn- ->event [turn]
  {:conversation-id (get turn "conversation_id")
   :turn-id (get turn "turn_id")
   :user-id (get turn "user_id" "anonymous")
   :text (get turn "text" "")
   :signal (get turn "signal")})

(defn- deliver-batch
  "Deliver a group of mutually `concurrent_with` turns: all are enqueued (arrive) in declared order
   without waiting for any to finish, so they are in flight together and the runtime must serialize
   them per conversation; the results come back in the declared order."
  [system turns]
  (let [outcomes (mapv #(core/submit-async system (->event %)) turns)]
    (mapv (fn [p] (let [r @p] (if (instance? Throwable r) (throw r) r))) outcomes)))

(defn- batches
  "Consecutive turns joined by `concurrent_with` form one batch; every other turn is its own."
  [turns]
  (loop [[t & more] turns out []]
    (cond
      (nil? t) out
      (seq (get t "concurrent_with"))
      (let [group (into #{(get t "turn_id")} (get t "concurrent_with"))
            [batch rest] (split-with #(group (get % "turn_id")) (cons t more))]
        (recur rest (conj out (vec batch))))
      :else (recur more (conj out [t])))))

(defn run-fixture
  "Run one fixture file. Returns {:id :status (:passed|:failed|:skipped) :problems [...] :results [...]}."
  [^java.io.File f]
  (let [fixture (read-fixture f)
        id (get fixture "id")
        missing (unsupported fixture)]
    (if (seq missing)
      {:id id :status :skipped :problems [(str "requires " (pr-str missing))]}
      (let [workflow (spec/load-workflow (workflow-of fixture f))
            stores (pipeline/open-stores workflow)
            system (atom (pipeline/system-from workflow stores))
            results (reduce (fn [acc batch]
                              (when (get (first batch) "restart_runtime")
                                (swap! system #(pipeline/system-from workflow (select-keys % [:store :state :log]))))
                              (into acc (deliver-batch @system batch)))
                            [] (batches (get fixture "turns")))
            wire (mapv log/->wire results)
            problems (vec (mapcat (fn [i expected]
                                    (let [actual (get wire i)
                                          errs (if actual
                                                 (into (spec/result-problems actual) (check-expectation expected actual))
                                                 [(str "no result for expectation " i)])]
                                      (map #(str "expect[" i "] " %) errs)))
                                  (range) (get fixture "expect")))]
        {:id id :status (if (seq problems) :failed :passed) :problems problems :results wire}))))

(defn run-all
  "Every fixture in spec order."
  []
  (mapv run-fixture (fixture-files)))

(defn report
  "A one-line-per-fixture summary."
  [outcomes]
  (str/join "\n" (map (fn [{:keys [id status problems]}]
                        (str (name status) " " id (when (seq problems) (str " — " (str/join "; " problems)))))
                      outcomes)))
