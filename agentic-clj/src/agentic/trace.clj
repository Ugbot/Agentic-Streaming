(ns agentic.trace
  "Phase 7: observability — a minimal tracing SPI, one span per turn / timer fire / CEP match. The
   noop tracer costs nothing; the recording tracer captures completed spans for tests and local
   inspection; an OpenTelemetry exporter is an opt-in adapter (the heavy dependency stays out of the
   core). Mirror of jagentic-core's org.jagentic.core.trace.* — behaviour at parity, expressed
   idiomatically with protocol+deftype (matching agentic.timers).

   A Span carries key/value attributes and named events, then is ended. attr/event are chainable
   (they return the span). A Tracer starts named spans.")

;; ---- protocols ----

(defprotocol Span
  "A single trace span — attach attributes and named events, then end it."
  (span-attr [_ k v] "Attach a string attribute; returns the span (chainable).")
  (span-event [_ n] "Record a named event; returns the span (chainable).")
  (span-end [_] "Close the span. Returns nil."))

(defprotocol Tracer
  "Begin a span by name; attributes/events are added via the returned Span, closed with span-end."
  (start [_ name] "Begin a span; returns a Span."))

;; ---- noop: spans do nothing ----

(deftype NoopSpan []
  Span
  (span-attr [this _ _] this)
  (span-event [this _] this)
  (span-end [_] nil))

(deftype NoopTracer [span]
  Tracer
  (start [_ _name] span))

(defn noop-tracer
  "A tracer whose spans do nothing — the zero-cost default."
  []
  (->NoopTracer (->NoopSpan)))

;; ---- recording: capture completed spans in end() order ----

;; A span mutates its attrs/events while open (an atom-backed map + vector), then on end appends an
;; immutable {:name :attrs :events} snapshot to the tracer's recorded vector.

(deftype RecordingSpan [name !attrs !events !recorded]
  Span
  (span-attr [this k v]
    (swap! !attrs assoc k v)
    this)
  (span-event [this n]
    (swap! !events conj n)
    this)
  (span-end [_]
    (swap! !recorded conj {:name name :attrs @!attrs :events @!events})
    nil))

(deftype RecordingTracer [!recorded]
  Tracer
  (start [_ name]
    (->RecordingSpan name (atom {}) (atom []) !recorded)))

(defn recording-tracer
  "A tracer that records completed spans in end() order — for tests and local inspection."
  []
  (->RecordingTracer (atom [])))

(defn spans
  "The completed spans of a recording tracer, in end() order: a vector of {:name :attrs :events}."
  [^RecordingTracer tracer]
  @(.-!recorded tracer))

(defn names
  "Span names of a recording tracer, in end() order."
  [tracer]
  (mapv :name (spans tracer)))
