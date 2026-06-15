(ns agentic.guardrail
  "Guardrails — {:check-input (fn [text] -> reason|nil) :check-output (fn [reply] -> reason|nil)} maps
   the graph screens turns with. Regex (deny patterns) + classifier (lexicon) flavours."
  (:require [clojure.string :as str]))

(defn regex-guardrail
  "Blocks when any deny pattern matches (case-insensitive)."
  [{:keys [deny reason check-outputs] :or {reason "blocked by policy"}}]
  (let [patterns (mapv #(re-pattern (str "(?i)" %)) deny)
        hit (fn [text] (when (and text (some #(re-find % text) patterns)) reason))]
    {:check-input hit
     :check-output (when check-outputs hit)}))

(defn- tokens [text] (re-seq #"[a-z0-9']+" (str/lower-case (or text ""))))

(defn lexicon-classify
  "Keyword-weighted classification → {:label :score :scores}. Mirror of LexiconClassifier."
  [lexicon default-label text]
  (let [toks (tokens text)
        n (count toks)]
    (if (zero? n)
      {:label default-label :score 0.0 :scores {}}
      (let [raw (into {} (map (fn [[label words]]
                                [label (/ (count (filter (set (map str/lower-case words)) toks)) (double n))])
                              lexicon))
            total (reduce + (vals raw))]
        (if (<= total 0.0)
          {:label default-label :score 0.0 :scores (zipmap (keys lexicon) (repeat 0.0))}
          (let [scores (into {} (map (fn [[k v]] [k (/ v total)]) raw))
                [label score] (apply max-key val scores)]
            {:label label :score score :scores scores}))))))

(defn classifier-guardrail
  "Blocks when a lexicon classifier assigns a blocked label above threshold."
  [{:keys [lexicon blocked threshold reason default-label check-outputs]
    :or {threshold 0.5 reason "blocked by classifier policy" default-label "other"}}]
  (let [blocked-set (set blocked)
        hit (fn [text]
              (when text
                (let [c (lexicon-classify lexicon default-label text)]
                  (when (and (blocked-set (:label c)) (>= (:score c) threshold))
                    (str reason " (" (:label c) "=" (format "%.2f" (:score c)) ")")))))]
    {:check-input hit
     :check-output (when check-outputs hit)}))
