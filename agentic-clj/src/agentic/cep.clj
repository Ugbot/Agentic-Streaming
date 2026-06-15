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
   exposed via `flush-expired` for timer-driven expiry).")

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
