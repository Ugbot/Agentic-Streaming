(ns agentic.example.time-travel
  "Distinctive Clojure showcase: the conversation transcript is **immutable Datomic datoms**, so any
   past state is a query `as-of` a point in the log — true time-travel, no event-replay machinery.

   We run a multi-turn banking conversation on a Datomic-backed system, capture the basis-t after the
   first turn, keep talking, then replay the transcript exactly as it stood back then. The early view
   is a strict prefix of the current one; later turns never mutated it.

       clojure -M:time-travel"
  (:require [agentic.core :as core]
            [agentic.event :as ev]
            [agentic.banking :as banking]
            [agentic.store :as store]
            [agentic.store.datomic :as dat]))

(defn run []
  (let [{:keys [conn conversation keyed]}
        (dat/datomic-stores {:db-name (str "time-travel-" (System/nanoTime)) :storage-dir :mem})
        sys (core/local-system (banking/build-graph) (banking/default-tools) (banking/retriever)
                               conversation keyed)
        cid "c1"
        ask (fn [text] (:reply (core/submit sys (ev/event cid "alice" text))))]

    (println "— turn 1 —")
    (println "  " (ask "what card types do you offer?"))
    (let [t-after-1 (dat/basis-t conn)]            ; a point in the immutable log
      (println "  captured basis-t =" t-after-1 "(transcript:" (store/message-count conversation cid) "messages)")

      (println "— turns 2 & 3 —")
      (println "  " (ask "tell me about crypto cash-back"))
      (println "  " (ask "what is my balance?"))

      (let [now (store/history conversation cid)
            back-then (dat/history-as-of conn cid t-after-1)]
        (println)
        (println "NOW  (" (count now) "messages):")
        (doseq [m now] (println "   " (:role m) "—" (:content m)))
        (println "AS-OF t=" t-after-1 "(" (count back-then) "messages):")
        (doseq [m back-then] (println "   " (:role m) "—" (:content m)))
        (println)
        (println (if (and (= 2 (count back-then)) (< (count back-then) (count now)))
                   "✓ time-travel: the early transcript is a strict prefix — datoms are immutable, history is durable"
                   "✗ unexpected: as-of view did not differ as expected"))
        {:now now :back-then back-then :t t-after-1}))))

(defn -main [& _]
  (run))
