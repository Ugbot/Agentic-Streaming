(ns agentic.cep
  "Phase 4 (HEADLINE): a portable, keyed NFA matcher over an event stream — the cross-engine
   counterpart of Flink CEP. Mirror of jagentic-core's org.jagentic.core.cep.* (CepMatcher /
   Pattern / Condition / Match) — behaviour at parity, expressed idiomatically as data + fns.

   A pattern is an ordered list of named stages, each with a contiguity, plus an optional `within`
   time bound on the whole match:
   - :begin        — the first stage.
   - :next         — strict: the immediately-next event must satisfy the condition, else the partial
                     is dropped.
   - :followed-by  — relaxed: non-matching events are skipped, the partial keeps waiting.

   A condition is a fn (fn [event matched-so-far] -> boolean). Simple conditions ignore
   matched-so-far; iterative conditions inspect the events already matched in the partial (the
   portable form of Flink's SimpleCondition / IterativeCondition).

   Feed events per key with `cep-match`; it advances partial matches and emits completed matches.
   `within` is enforced by expiring partials whose first event is older than the bound (also
   exposed via `flush-expired` for timer-driven expiry)."
  (:require [clojure.string :as str]
            [agentic.event :as event]
            [agentic.tools :as tools]))

;; ---- conditions ----

(defn simple-cond
  "Wrap an event-only predicate (fn [event] ...) as a stage condition (fn [event matched-so-far] ...)
   — the portable form of Flink's SimpleCondition."
  [pred]
  (fn [event _matched-so-far] (boolean (pred event))))

(def any-cond
  "A condition that always matches."
  (fn [_event _matched-so-far] true))

;; ---- pattern builders (data maps; fluent via threading) ----

(defn begin
  "Start a pattern with one :begin stage."
  [name condition]
  {:stages [{:name name :contiguity :begin :condition condition}]
   :within-millis 0})

(defn pnext
  "Append a strict (:next) stage. The immediately-next event must satisfy `condition`, else the
   partial is dropped. Named `pnext` to avoid clashing with clojure.core/next."
  [pattern name condition]
  (update pattern :stages conj {:name name :contiguity :next :condition condition}))

(defn followed-by
  "Append a relaxed (:followed-by) stage. Non-matching events are skipped; the partial keeps waiting."
  [pattern name condition]
  (update pattern :stages conj {:name name :contiguity :followed-by :condition condition}))

(defn within
  "Bound the whole match to `millis` from the first matched event (0 = unbounded)."
  [pattern millis]
  (assoc pattern :within-millis millis))

;; ---- match construction ----

(defn- ->match
  "Build a completed match {:events [..] :named {name event ...}} from the matched events, naming
   each by its stage (in stage order, preserving insertion order via array-map)."
  [events stages]
  (let [named (->> (map vector stages events)
                   (reduce (fn [m [stage ev]] (assoc m (:name stage) ev)) (array-map)))]
    {:events events :named named}))

;; ---- matcher ----

(defn cep-matcher
  "Build a stateful, keyed NFA matcher for `pattern`. Atom-backed: {key -> [partial ...]} where each
   partial = {:events [..] :stage i :start-ts t}. Drive it with `cep-match` / `flush-expired`."
  [pattern]
  {::kind :cep-matcher
   :pattern pattern
   :state (atom {})})

(defn cep-match
  "Feed one `event` for `key` at logical time `ts`; return a vector of completed matches. Advances
   existing partials one stage when the next stage's condition matches (a :next partial is dropped on
   non-match, a :followed-by partial waits), and starts a fresh partial at stage 0 whenever the first
   stage's condition matches. Mirrors org.jagentic.core.cep.CepMatcher#match exactly."
  [matcher key ts event]
  (let [{:keys [pattern state]} matcher
        stages (:stages pattern)
        within (:within-millis pattern)
        last-idx (dec (count stages))
        completed (volatile! [])]
    ;; Compute survivors + completed atomically. swap! may retry, so it must be pure: the volatile
    ;; is reset to the empty vector at the top of each attempt before being populated.
    (swap! state
           (fn [by-key]
             (vreset! completed [])
             (let [partials (get by-key key [])
                   ;; within>0 → drop partials whose first event is older than the bound.
                   live (if (pos? within)
                          (filterv #(<= (- ts (:start-ts %)) within) partials)
                          partials)
                   survivors
                   (reduce
                    (fn [acc p]
                      (let [next-stage (inc (:stage p))
                            stage (nth stages next-stage)]
                        (if ((:condition stage) event (:events p))
                          (let [advanced (conj (:events p) event)]
                            (if (= next-stage last-idx)
                              (do (vswap! completed conj (->match advanced stages))
                                  acc)
                              (conj acc {:events advanced :stage next-stage :start-ts (:start-ts p)})))
                          (if (= (:contiguity stage) :followed-by)
                            (conj acc p)        ; relaxed: skip this event, keep waiting
                            acc))))             ; :next (strict) + non-match → drop p
                    []
                    live)
                   ;; Every event may also start a new partial at stage 0.
                   first-stage (nth stages 0)
                   survivors (if ((:condition first-stage) event [])
                               (let [ev [event]]
                                 (if (= last-idx 0)
                                   (do (vswap! completed conj (->match ev stages))
                                       survivors)
                                   (conj survivors {:events ev :stage 0 :start-ts ts})))
                               survivors)]
               (assoc by-key key survivors))))
    @completed))

(defn flush-expired
  "Remove and return the matched-events (a vector of events-vectors) of partials for `key` that have
   exceeded `within` as of `now` — the portable form of Flink's timed-out partial matches. Empty if
   the pattern is unbounded. Mirrors org.jagentic.core.cep.CepMatcher#flushExpired exactly."
  [matcher key now]
  (let [{:keys [pattern state]} matcher
        within (:within-millis pattern)]
    (if-not (pos? within)
      []
      (let [out (volatile! [])]
        (swap! state
               (fn [by-key]
                 (vreset! out [])
                 (let [partials (get by-key key)]
                   (if (nil? partials)
                     by-key
                     (let [survivors (reduce
                                      (fn [acc p]
                                        (if (> (- now (:start-ts p)) within)
                                          (do (vswap! out conj (:events p)) acc)
                                          (conj acc p)))
                                      []
                                      partials)]
                       (assoc by-key key survivors))))))
        @out))))

;; ---- declarative compiler: weave a pipeline `cep:` section into runnable wirings ----
;;
;; Mirror of jagentic-core's org.jagentic.core.cep.CepSpec / CepWiring. A `cep:` section is a list of
;; specs (string keys, from YAML/EDN):
;;   {"name" "host_incident"
;;    "key"  "conversation_id"            ; conversation_id | conversationId | metadata.<field>
;;    "ts"   "metadata.ts"                ; metadata.<field> (else a per-rule arrival counter)
;;    "within" 300000
;;    "pattern" [{"stage" "first"  "where" {"text_contains" "anomaly"}}
;;               {"stage" "second" "where" {"text_contains" "anomaly"} "contiguity" "followedBy"}
;;               {"stage" "third"  "where" {"text_contains" "anomaly"} "contiguity" "followedBy"}]
;;    "on_match" {"kind" "submit" "text" "incident on {key}"}}   ; or {"kind" "tool" "tool" id "args" {..}}
;;
;; `where`: "any"/nil · {"text_contains" s|[..]} · {"metadata_equals" {k v}} · {"metadata_gt" {k n}}.

(def derived-key
  "Metadata flag marking an event injected by a CEP action (so CEP does not re-match it). Mirror of
   org.jagentic.core.cep.CepWiring/DERIVED."
  "__cep_derived__")

(defn- ->seq
  "Coerce a scalar-or-list into a seq of strings (text_contains may be one needle or many)."
  [v]
  (cond (sequential? v) (map str v) (nil? v) [] :else [(str v)]))

(defn- spec-condition
  "Compile a `where` clause into a stage condition (fn [event matched-so-far] -> boolean)."
  [where]
  (cond
    (or (nil? where) (= "any" where)) any-cond

    (map? where)
    (cond
      (contains? where "text_contains")
      (let [needles (->seq (get where "text_contains"))]
        (simple-cond (fn [e] (when-let [t (:text e)] (boolean (some #(str/includes? t %) needles))))))

      (contains? where "metadata_equals")
      (let [kv (get where "metadata_equals")]
        (simple-cond (fn [e] (every? (fn [[k v]] (= (str v) (get-in e [:metadata k]))) kv))))

      (contains? where "metadata_gt")
      (let [kv (get where "metadata_gt")]
        (simple-cond (fn [e] (every? (fn [[k n]]
                                       (try
                                         (> (Double/parseDouble (str (get-in e [:metadata k])))
                                            (double n))
                                         (catch Exception _ false)))
                                     kv))))

      :else (throw (ex-info (str "unknown cep where: " where) {:where where})))

    :else (throw (ex-info (str "unknown cep where: " where) {:where where}))))

(defn- spec-pattern
  "Build a pattern from the stages list + `within-millis`. contiguity \"next\" → :next (strict),
   else :followed-by (relaxed). The first stage is always :begin."
  [stages within-millis]
  (when (empty? stages)
    (throw (ex-info "cep pattern needs at least one stage" {})))
  (-> (reduce
       (fn [pattern st]
         (let [stage (str (get st "stage" "s"))
               cnd (spec-condition (get st "where"))]
           (if (nil? pattern)
             (begin stage cnd)
             (if (= "next" (str/lower-case (str (get st "contiguity" "followedBy"))))
               (pnext pattern stage cnd)
               (followed-by pattern stage cnd)))))
       nil
       stages)
      (within (or within-millis 0))))

(defn- spec-key-fn
  "Compile a `key` selector into (fn [event] -> string). conversation_id / conversationId / default →
   :conversation-id; metadata.<field> → (get-in event [:metadata field])."
  [key]
  (let [k (str (or key "conversation_id"))]
    (if (str/starts-with? k "metadata.")
      (let [field (subs k (count "metadata."))]
        (fn [event] (get-in event [:metadata field])))
      :conversation-id)))

(defn- spec-ts-fn
  "Compile a `ts` selector into (fn [event] -> long). metadata.<field> → parse long (default 0);
   else a monotonic per-rule arrival counter."
  [ts]
  (if (and ts (str/starts-with? (str ts) "metadata."))
    (let [field (subs (str ts) (count "metadata."))]
      (fn [event]
        (let [v (get-in event [:metadata field])]
          (if (nil? v) 0 (try (Long/parseLong (str v)) (catch Exception _ 0))))))
    (let [counter (atom -1)]
      (fn [_event] (swap! counter inc)))))

(defn- spec-action
  "Compile an `on_match` clause into an action (fn [match key submit-fn tools] -> any). kind \"submit\"
   → submit a derived event (text with {key} substituted, DERIVED metadata); kind \"tool\" → execute a
   tool. nil on_match → detect-only no-op. Mirror of CepSpec#action.

   `submit-fn` is the INNER core submit bound to the system (so a submit action does not re-feed CEP);
   the DERIVED tag is a second guard. `tools` is the tool registry."
  [on-match]
  (if (nil? on-match)
    (fn [_match _key _submit-fn _tools] nil)
    (let [kind (str (get on-match "kind" "submit"))]
      (case kind
        "tool" (let [tool-id (str (get on-match "tool"))
                     args (get on-match "args" {})]
                 (fn [_match _key _submit-fn tools] (tools/execute tools tool-id args)))
        "submit" (let [text (str (get on-match "text" "cep match"))]
                   (fn [_match key submit-fn _tools]
                     (let [body (str/replace text "{key}" (if (nil? key) "" (str key)))]
                       (submit-fn (event/event key "cep" body {derived-key "true"})))))
        (throw (ex-info (str "unknown cep on_match kind: " kind) {:kind kind}))))))

(defn compile-cep
  "Compile a pipeline `cep:` section (a list of specs, string keys) into a vector of wiring maps
   {:name :matcher :key-fn :ts-fn :action}. Mirror of org.jagentic.core.cep.CepSpec/compile."
  [specs]
  (mapv (fn [s]
          {:name (str (get s "name" "cep"))
           :matcher (cep-matcher (spec-pattern (get s "pattern") (get s "within" 0)))
           :key-fn (spec-key-fn (get s "key"))
           :ts-fn (spec-ts-fn (get s "ts"))
           :action (spec-action (get s "on_match"))})
        specs))

(defn cep-on-event
  "Feed one inbound `event` to a single compiled wiring `w`, firing its action for every completed
   match. Events produced by a CEP action (tagged DERIVED) are skipped, so a submit action cannot
   recurse. `submit-fn` is the inner core submit (NOT the CEP-feeding pipeline submit); `tools` is the
   tool registry. Mirror of org.jagentic.core.cep.CepWiring#onEvent."
  [w event submit-fn tools]
  (when-not (get-in event [:metadata derived-key])
    (let [{:keys [matcher key-fn ts-fn action]} w
          key (key-fn event)]
      (doseq [m (cep-match matcher key (ts-fn event) event)]
        (action m key submit-fn tools)))))

;; ---- observer: adapt the matcher to the agentic.stream observer seam ----

(defn cep-observer
  "Build an observer fn (fn [event] ...) suitable for agentic.stream/observe. For each event it derives
   a key via `key-fn`, a logical time via `ts-fn`, feeds the matcher with `cep-match`, and calls
   `on-match` once per completed match. The matcher is created internally and shared across calls."
  [pattern key-fn ts-fn on-match]
  (let [matcher (cep-matcher pattern)]
    (fn [event]
      (doseq [m (cep-match matcher (key-fn event) (ts-fn event) event)]
        (on-match m)))))
