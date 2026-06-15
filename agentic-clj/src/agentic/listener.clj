(ns agentic.listener
  "Lifecycle hooks. A listener is a map of hook-keyword -> fn; fire invokes each that has the hook.
   Hooks: :on-turn-start :on-routed :on-tool-call-start :on-tool-call-end :on-guardrail-block
   :on-error :on-turn-end. Mirror of jagentic-core AgentListener/CompositeListener.")

(defn fire [listeners hook event]
  (doseq [l listeners]
    (when-let [f (get l hook)]
      (try (f event) (catch Throwable _ nil))))
  nil)
