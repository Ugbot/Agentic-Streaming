(ns agentic.windows
  "Phase 3: portable keyed time-windows — the counterpart of Flink's window assigners (a Pekko
   per-key fold, a Temporal aggregate). Each window is stateful and keyed: events are partitioned
   by key and aggregated independently. Mirror of jagentic-core's org.jagentic.core.windows.* —
   behaviour at parity, expressed idiomatically.

   Three flavours:
   - sliding-window  — count + sum within (ts-window, ts] for a key (the VelocityDetector:
                       \"5 payments on one account in 60s\").
   - tumbling-window — fixed, non-overlapping buckets floor(ts/window); a later-bucket event closes
                       and emits the open bucket.
   - session-window  — events grouped into sessions separated by an inactivity gap; an event arriving
                       more than gap after the previous one closes (emits) the prior session.

   An aggregate is a plain map. Each window is backed by an atom (like agentic.retrieval), with plain
   fns taking the window as the first argument. Independent per key.

   NOTE: division uses Clojure's floor-division semantics. Clojure's (quot) truncates toward zero,
   which differs from Java's Math.floorDiv for negative timestamps; we use (Math/floorDiv) to match
   the Java core exactly (negative ts land in the bucket below, not toward zero).")

;; ---- sliding window — keyed persistent queues of [ts value] entries ----
;; State is fully immutable (a persistent vector per key) so the atom's swap! is retry-safe.

(defn sliding-window
  "A keyed sliding time-window over `window-millis`. State is an atom of {key -> [[ts value] ...]}."
  [window-millis]
  {::kind :sliding
   :window-millis window-millis
   :state (atom {})})

(defn slide-add
  "Record an event for `key` at `ts`; evict entries with entry-ts <= ts - window-millis, then return
   {:count n :sum s} of what remains. `value` defaults to 1.0."
  ([w key ts] (slide-add w key ts 1.0))
  ([w key ts value]
   (let [window-millis (:window-millis w)
         cutoff (- ts window-millis)
         ;; Append, then drop the leading entries that have aged out. Entries arrive in ts order in
         ;; the streaming model, so a prefix-drop matches the Java deque eviction.
         next-q (fn [q]
                  (->> (conj (or q []) [ts (double value)])
                       (drop-while (fn [[ets _]] (<= (long ets) cutoff)))
                       vec))
         updated (swap! (:state w) update key next-q)
         q (get updated key)]
     {:count (count q)
      :sum (reduce + 0.0 (map second q))})))

;; ---- tumbling window — keyed open bucket {:index :count :sum} ----

(defn tumbling-window
  "A keyed tumbling (fixed, non-overlapping) time-window over `window-millis`. State is an atom of
   {key -> {:index :count :sum}} for the currently-open bucket per key."
  [window-millis]
  {::kind :tumbling
   :window-millis window-millis
   :state (atom {})})

(defn- bucket-start
  [index window-millis]
  (* index window-millis))

(defn- closed-bucket
  [key open window-millis]
  {:key key
   :start (bucket-start (:index open) window-millis)
   :count (:count open)
   :sum (:sum open)})

(defn tumble-add
  "Add an event for `key` at `ts`. If it falls in a later bucket than the open one, close and return
   the previous bucket {:key :start :count :sum}; otherwise return nil. `value` defaults to 1.0.

   Retry-safe: `swap-vals!` gives us the pre-swap state, so the emitted bucket is derived from the old
   open bucket after the swap commits — no in-fn side effects."
  ([w key ts] (tumble-add w key ts 1.0))
  ([w key ts value]
   (let [window-millis (:window-millis w)
         index (Math/floorDiv (long ts) (long window-millis))
         [old _] (swap-vals!
                  (:state w)
                  (fn [by-key]
                    (let [open (get by-key key)]
                      (if (or (nil? open) (> index (:index open)))
                        (assoc by-key key {:index index :count 1 :sum (double value)})
                        (assoc by-key key (-> open
                                              (update :count inc)
                                              (update :sum + (double value))))))))
         open (get old key)]
     (when (and open (> index (:index open)))
       (closed-bucket key open window-millis)))))

(defn tumble-close
  "Flush the currently-open bucket for `key`, returning {:key :start :count :sum} or nil if none open."
  [w key]
  (let [window-millis (:window-millis w)
        [old _] (swap-vals! (:state w) dissoc key)
        open (get old key)]
    (when open (closed-bucket key open window-millis))))

;; ---- session window — keyed open session {:start :last :count :sum} ----

(defn session-window
  "A keyed session window with inactivity `gap-millis`. State is an atom of
   {key -> {:start :last :count :sum}} for the currently-open session per key."
  [gap-millis]
  {::kind :session
   :gap-millis gap-millis
   :state (atom {})})

(defn- closed-session
  [key open]
  {:key key
   :start (:start open)
   :end (:last open)
   :count (:count open)
   :sum (:sum open)})

(defn- gap-exceeded?
  [open ts gap-millis]
  (and open (> (- ts (:last open)) gap-millis)))

(defn session-add
  "Add an event for `key` at `ts`. If it arrives more than `gap-millis` after the previous one, close
   and return the prior session {:key :start :end :count :sum}; otherwise return nil. `value` defaults
   to 1.0.

   Retry-safe: derive the emitted session from the pre-swap state via `swap-vals!`."
  ([w key ts] (session-add w key ts 1.0))
  ([w key ts value]
   (let [gap-millis (:gap-millis w)
         [old _] (swap-vals!
                  (:state w)
                  (fn [by-key]
                    (let [open (get by-key key)
                          ;; Close the prior session if the gap is exceeded; then (re)open.
                          open (when-not (gap-exceeded? open ts gap-millis) open)
                          open (or open {:start ts :last ts :count 0 :sum 0.0})]
                      (assoc by-key key (-> open
                                            (assoc :last ts)
                                            (update :count inc)
                                            (update :sum + (double value)))))))
         prior (get old key)]
     (when (gap-exceeded? prior ts gap-millis)
       (closed-session key prior)))))

(defn session-close
  "Flush the currently-open session for `key`, returning {:key :start :end :count :sum} or nil if none."
  [w key]
  (let [[old _] (swap-vals! (:state w) dissoc key)
        open (get old key)]
    (when open (closed-session key open))))
