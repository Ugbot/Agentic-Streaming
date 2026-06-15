(ns agentic.context-window
  "Context-window management — MoSCoW compaction so a transcript fits a token budget. Keep MUST
   first, then SHOULD, then COULD; drop WONT; stop at the budget; preserve input order among kept
   items. Mirror of jagentic-core ContextWindowManager. Items are {:text :priority} with priority
   in #{:wont :could :should :must}.")

(def ^:private priority-rank {:wont 0 :could 1 :should 2 :must 3})

(defn tokens
  "Cheap ~4-chars/token estimate."
  [text]
  (max 1 (quot (+ (count text) 3) 4)))

(defn compact
  "Return the kept items (original order) whose total token estimate fits max-tokens, dropping
   :wont and preferring higher priority."
  [items max-tokens]
  (let [budget (max 1 max-tokens)
        candidates (vec (keep-indexed (fn [i it] (when (not= :wont (:priority it)) [i it]))
                                      items))
        ;; choose which to keep: descending priority, stable on original index
        order (sort-by (fn [[i it]] [(- (priority-rank (:priority it) 0)) i]) candidates)
        kept (loop [left budget [pair & more] order keep #{}]
               (if (nil? pair)
                 keep
                 (let [[i it] pair t (tokens (:text it))]
                   (if (<= t left)
                     (recur (- left t) more (conj keep i))
                     (recur left more keep)))))]
    (->> candidates
         (filter (fn [[i _]] (kept i)))
         (map second)
         vec)))
