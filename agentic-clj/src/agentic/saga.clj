(ns agentic.saga
  "Compensation/saga — register reversible steps; on a later failure run the recorded undos in
   reverse (LIFO). Mirror of jagentic-core Saga. Uses an atom of recorded compensations.")

(declare compensate)

(defn saga [] (atom []))

(defn step
  "Run do-fn; on success record undo-fn for rollback and return the result. If do-fn throws,
   compensate everything recorded so far and re-throw."
  [sg name do-fn undo-fn]
  (let [result (try (do-fn)
                    (catch Throwable e
                      (compensate sg)
                      (throw e)))]
    (swap! sg conj {:name name :undo undo-fn})
    result))

(defn compensate
  "Run recorded undos in reverse; returns the compensated step names."
  [sg]
  (let [steps @sg]
    (reset! sg [])
    (vec (for [{:keys [name undo]} (reverse steps)]
           (do (when undo (undo)) name)))))
