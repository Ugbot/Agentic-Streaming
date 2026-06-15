(ns agentic.suspend
  "Human-in-the-loop suspend/resume — the Clojure mirror of jagentic-core's suspend package.
   A turn that needs-approval? suspends instead of completing; a later resume (CQRS: resume is just
   another command) replays the held turn (approved) or denies it. check-timeouts escalates
   suspensions that age past the timeout — the portable 'approve within N or escalate' pattern,
   composing with the Phase-2 timer service.

   A suspension is a map {:conversation-id :reason :pending-text :since}. The suspension-service is
   atom-backed (atom of {cid -> suspension}); the in-memory default is process-local, mirroring the
   Java InMemory impl (a durable impl would persist via the keyed-state store, as timers do)."
  (:require [agentic.core :as core]
            [agentic.event :as ev]))

;; ---------------------------------------------------------------------------
;; Suspension service (atom of {cid -> suspension})
;; ---------------------------------------------------------------------------

(defn suspension-service
  "An atom-backed suspension service: an atom of {conversation-id -> suspension}."
  []
  (atom {}))

(defn suspend
  "Record a suspension for cid. Returns the suspension."
  [svc cid reason pending-text now]
  (let [s {:conversation-id cid :reason reason :pending-text pending-text :since now}]
    (swap! svc assoc cid s)
    s))

(defn suspended?
  "True when cid currently has a pending suspension."
  [svc cid]
  (contains? @svc cid))

(defn peek-suspension
  "The pending suspension for cid without clearing it, or nil."
  [svc cid]
  (get @svc cid))

(defn clear-suspension
  "Remove and return the pending suspension for cid (the resume command), or nil if none."
  [svc cid]
  (let [s (get @svc cid)]
    (when s
      (swap! svc dissoc cid))
    s))

(defn all-pending
  "All currently-suspended suspensions (for timeout sweeps), as a vector."
  [svc]
  (vec (vals @svc)))

;; ---------------------------------------------------------------------------
;; Human gate
;; ---------------------------------------------------------------------------

(defn- result
  [cid reply path ok]
  {:conversation-id cid :reply reply :path path :ok ok :tool-calls []})

(defn human-gate
  "A human-in-the-loop gate around a system (what core/submit takes). needs-approval? is
   (fn [event] -> boolean). timeout-millis (0 / nil = no timeout) bounds how long a suspension may
   age before check-timeouts escalates it. Returns a gate map."
  ([system svc needs-approval?]
   (human-gate system svc needs-approval? 0))
  ([system svc needs-approval? timeout-millis]
   {:system system
    :suspensions svc
    :needs-approval? needs-approval?
    :timeout-millis (or timeout-millis 0)}))

(defn gate-submit
  "Submit through the gate. Suspends (awaiting approval) when required or already pending; otherwise
   a normal turn via core/submit."
  [gate event now]
  (let [{:keys [system suspensions needs-approval?]} gate
        cid (:conversation-id event)]
    (cond
      (suspended? suspensions cid)
      (result cid "[awaiting-approval] a turn is already pending approval" "awaiting-approval" false)

      (needs-approval? event)
      (let [text (:text event)]
        (suspend suspensions cid (str "approval required: " text) text now)
        (result cid (str "[awaiting-approval] " text) "awaiting-approval" false))

      :else
      (core/submit system event))))

(defn gate-resume
  "Resume a suspended conversation: approved? → replay the held turn as a fresh metadata-free event
   (so it won't re-trigger the gate); denied → report; nothing pending → report."
  [gate cid approved? now]
  (let [{:keys [system suspensions]} gate
        pending (clear-suspension suspensions cid)]
    (cond
      (nil? pending)
      (result cid "[resume] nothing pending" "resume" false)

      (not approved?)
      (result cid (str "[denied] " (:reason pending)) "denied" false)

      :else
      (core/submit system (ev/event cid "system" (:pending-text pending))))))

(defn gate-check-timeouts
  "Escalate (and clear) suspensions older than the timeout; one turn-result per escalated
   conversation. No-op (empty) when timeout-millis is nil/0."
  [gate now]
  (let [{:keys [suspensions timeout-millis]} gate]
    (if (or (nil? timeout-millis) (zero? timeout-millis))
      []
      (vec
       (for [s (all-pending suspensions)
             :when (> (- now (:since s)) timeout-millis)]
         (do
           (clear-suspension suspensions (:conversation-id s))
           (result (:conversation-id s)
                   (str "[escalated] approval timed out: " (:reason s))
                   "escalated" false)))))))
