(ns agentic.main
  "Banking REPL/CLI demo: clojure -M:run"
  (:require [agentic.core :as core]
            [agentic.event :as ev]))

(defn -main [& _]
  (let [sys (core/banking-system)]
    (doseq [e [(ev/event "c1" "alice" "what card types do you offer?")
               (ev/event "c2" "bob" "what is my balance?")
               (ev/event "c1" "alice" "tell me about crypto cash-back")
               (ev/event "c3" "carol" "hello there")]]
      (let [r (core/submit sys e)]
        (println (format "[%s] path=%-8s ok=%s reply=%s"
                         (:conversation-id e) (:path r) (:ok r) (:reply r)))))))
