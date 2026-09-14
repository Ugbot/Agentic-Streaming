(ns agentic.cep-fold
  "The agentic/v1 `cep:` sequence patterns as a pure fold over one conversation's event log
   (mirror of org.jagentic.core.cep.SequencePattern and pyagentic agentic.cep).

   A pattern with `on_match.kind: tool` is evaluated inside the turn, after `routed` and before the
   brain, over the `turn_received` events of the conversation so far. Stages are tried in declaration
   order; `where.text_contains` is a case-insensitive substring test; a `next` stage must match the
   turn right after the previous stage's turn while `followedBy` tolerates non-matching turns in
   between; `within` bounds the event time between the first and last matched turn, read from the
   metadata field named by `ts`. A completed match consumes its turns, so matches never overlap. When
   the last turn of the log completes a match the pattern's tool is invoked through `ctx/call-tool`
   with `{\"pattern\" name \"key\" conversation-id}`, so its `tool_called` lands on that turn.

   Rules whose action is not a tool (`submit`, detect-only) stay with agentic.cep, the post-turn
   matcher; `without-tool-actions` selects them."
  (:require [clojure.string :as str]
            [agentic.context :as ctx]))

(def tool-kind "tool")

(def event-time-key
  "The turn metadata field the conformance fixtures carry event time in (milliseconds, a string)."
  "event_time_ms")

(defn event-time-ms
  "The single read of a turn's event time: `metadata.event_time_ms` parsed as a long, nil when the
   turn carries none. Throws a :validation error when the value is not an integer."
  [metadata]
  (when-some [raw (get metadata event-time-key)]
    (try (Long/parseLong (str/trim (str raw)))
         (catch NumberFormatException _
           (throw (ex-info (str "metadata." event-time-key " is not an integer: " (pr-str raw))
                           {:error/class :validation}))))))

;; ---- compilation ----

(defn- kv
  "Read `k` from a rule fragment in canonical (kebab keyword) or raw (string) form."
  ([m k] (kv m k nil))
  ([m k default]
   (let [kebab (keyword (str/replace (name k) "_" "-"))]
     (cond (contains? m kebab) (get m kebab)
           (contains? m (name k)) (get m (name k))
           :else default))))

(defn tool-action?
  "True when the rule's `on_match` invokes a tool."
  [rule]
  (let [on-match (kv rule :on_match)]
    (and (map? on-match) (= tool-kind (str (kv on-match :kind "submit"))))))

(defn without-tool-actions
  "The rules `compile-patterns` does not take: everything without a tool action."
  [rules]
  (vec (remove tool-action? rules)))

(defn- stage-of [rule-name i st]
  (let [where (kv st :where)
        needle (when (map? where) (kv where :text_contains))
        contiguity (str/lower-case (str (kv st :contiguity "next")))]
    (when (and (some? needle) (not (string? needle)))
      (throw (ex-info (str "cep pattern " rule-name " stage " i ": text_contains must be a string")
                      {:error/class :validation})))
    (when-not (#{"next" "followedby"} contiguity)
      (throw (ex-info (str "cep pattern " rule-name " stage " i ": unknown contiguity " (pr-str (kv st :contiguity)))
                      {:error/class :validation})))
    {:name (str (or (kv st :stage) (str "stage" i)))
     :text-contains (some-> needle str/lower-case)
     :contiguity (if (= "next" contiguity) :next :followed-by)}))

(defn compile-pattern
  "One `cep:` rule (canonical or raw) with a tool action to
   {:name :stages :tool :ts-key :within-ms}."
  [rule]
  (let [rule-name (str (or (kv rule :name) "cep"))
        key-sel (str (or (kv rule :key) "conversation_id"))
        ts (kv rule :ts)
        within (kv rule :within)
        stages (kv rule :pattern)
        tool (kv (kv rule :on_match) :tool)]
    (when-not (#{"conversation_id" "conversationId"} key-sel)
      (throw (ex-info (str "cep pattern " rule-name ": in-turn matching is keyed by conversation_id, not " key-sel)
                      {:error/class :validation})))
    (when (and (some? ts) (not (str/starts-with? (str ts) "metadata.")))
      (throw (ex-info (str "cep pattern " rule-name ": ts must name a metadata field, got " (pr-str ts))
                      {:error/class :validation})))
    (when (and (some? within) (nil? ts))
      (throw (ex-info (str "cep pattern " rule-name ": within needs ts") {:error/class :validation})))
    (when (empty? stages)
      (throw (ex-info (str "cep pattern " rule-name ": pattern needs at least one stage") {:error/class :validation})))
    (when (str/blank? (str tool))
      (throw (ex-info (str "cep pattern " rule-name ": on_match.tool is required") {:error/class :validation})))
    {:name rule-name
     :stages (vec (map-indexed (partial stage-of rule-name) stages))
     :tool (str tool)
     :ts-key (when ts (subs (str ts) (count "metadata.")))
     :within-ms (when (some? within) (long within))}))

(defn compile-patterns
  "The rules evaluated in-turn: those with `on_match.kind: tool`."
  [rules]
  (mapv compile-pattern (filter tool-action? rules)))

;; ---- the fold ----

(defn stage-matches? [stage text]
  (or (nil? (:text-contains stage))
      (str/includes? (str/lower-case (or text "")) (:text-contains stage))))

(defn timestamp
  "The event time of a turn under this pattern's `ts`: metadata.<ts-key> parsed as a long."
  [pattern turn]
  (let [k (:ts-key pattern)
        raw (get (:metadata turn) k)]
    (when (nil? raw)
      (throw (ex-info (str "turn " (:turn-id turn) " lacks metadata." k " needed by cep pattern " (:name pattern))
                      {:error/class :validation})))
    (try (Long/parseLong (str/trim (str raw)))
         (catch NumberFormatException _
           (throw (ex-info (str "turn " (:turn-id turn) " metadata." k " is not an integer: " (pr-str raw))
                           {:error/class :validation}))))))

(defn completed-indexes
  "Indexes (into `turns`) of the turns that complete a match, folding left to right with one partial
   match: a turn past the window resets the partial and is offered to stage one; a stage-one match
   starts the partial; a non-match after a `next` stage drops the partial; a completed match is
   consumed and the partial restarts empty."
  [pattern turns]
  (let [stages (:stages pattern)
        n (count stages)
        within (:within-ms pattern)]
    (loop [i 0 stage-index 0 start-ts 0 done []]
      (if (= i (count turns))
        done
        (let [turn (nth turns i)
              stage-index (if (and (pos? stage-index) within (> (- (timestamp pattern turn) start-ts) within))
                            0
                            stage-index)
              stage (nth stages stage-index)]
          (cond
            (stage-matches? stage (:text turn))
            (let [start-ts (if (and (zero? stage-index) (:ts-key pattern)) (timestamp pattern turn) start-ts)
                  next-index (inc stage-index)]
              (if (= next-index n)
                (recur (inc i) 0 start-ts (conj done i))
                (recur (inc i) next-index start-ts done)))

            (and (pos? stage-index) (= :next (:contiguity stage)))
            (recur (inc i) 0 start-ts done)

            :else (recur (inc i) stage-index start-ts done)))))))

(defn completes-on?
  "True when the last of `turns` completes a match of `pattern`."
  [pattern turns]
  (boolean (and (seq turns) (= (dec (count turns)) (peek (completed-indexes pattern turns))))))

;; ---- from the log, into the turn ----

(defn turns-of
  "The conversation's turns, in order, from its `turn_received` events:
   [{:turn-id :text :metadata}]. Metadata values are strings."
  [events]
  (into [] (comp (filter #(= :turn-received (:type %)))
                 (map (fn [{:keys [payload]}]
                        {:turn-id (:turn-id payload)
                         :text (or (:text payload) "")
                         :metadata (into {} (map (fn [[k v]] [(name k) (str v)])) (:metadata payload))})))
        events))

(defn match-args
  "The arguments a completing pattern passes to its tool."
  [pattern conversation-id]
  {"pattern" (:name pattern) "key" conversation-id})

(defn evaluate!
  "Fold every pattern over `events` (the conversation's log including this turn's `turn_received`);
   each one the last turn completes invokes its tool through `ctx/call-tool`, in declaration order.
   Tool failures propagate as `ctx/tool-error`."
  [patterns c events]
  (when (seq patterns)
    (let [turns (turns-of events)]
      (doseq [p patterns :when (completes-on? p turns)]
        (ctx/call-tool c (:tool p) (match-args p (:conversation-id c)))))))
