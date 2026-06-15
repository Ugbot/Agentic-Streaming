(ns agentic.store
  "Storage SPIs — the per-conversation transcript/attributes, keyed scalar state, and long-term
   archive. In-memory (atom) impls are the model-free default; agentic.store.datomic backs the same
   protocols with Datomic. Mirror of the jagentic-core ConversationStore/KeyedStateStore/LongTermStore.")

(defprotocol ConversationStore
  (append [this cid msg] "Append a transcript message {:role :content :tool-name :tool-call-id}.")
  (history [this cid] "Ordered transcript messages for the conversation.")
  (message-count [this cid])
  (put-attribute [this cid k v] "Set a scalar attribute (e.g. phase, path).")
  (get-attribute [this cid k])
  (attributes [this cid])
  (associate-user [this cid uid])
  (conversations-for-user [this uid])
  (clear-conversation [this cid]))

(defprotocol KeyedStateStore
  (kv-get [this k name])
  (kv-put [this k name v])
  (kv-clear [this k]))

(defprotocol LongTermStore
  (save-turn [this cid uid role content])
  (load-history [this cid] "Vector of [role content] pairs.")
  (save-fact [this uid k v])
  (facts [this uid])
  (lt-conversations-for-user [this uid]))

;; ---- in-memory impls ----

(defrecord InMemoryConversationStore [!state]
  ConversationStore
  (append [_ cid msg] (swap! !state update-in [cid :messages] (fnil conj []) msg))
  (history [_ cid] (get-in @!state [cid :messages] []))
  (message-count [_ cid] (count (get-in @!state [cid :messages] [])))
  (put-attribute [_ cid k v] (swap! !state assoc-in [cid :attrs k] v))
  (get-attribute [_ cid k] (get-in @!state [cid :attrs k]))
  (attributes [_ cid] (get-in @!state [cid :attrs] {}))
  (associate-user [_ cid uid] (swap! !state assoc-in [cid :user] uid))
  (conversations-for-user [_ uid]
    (->> @!state (filter (fn [[_ v]] (= uid (:user v)))) (map key) vec))
  (clear-conversation [_ cid] (swap! !state dissoc cid)))

(defn in-memory-conversation-store []
  (->InMemoryConversationStore (atom {})))

(defrecord InMemoryKeyedStateStore [!state]
  KeyedStateStore
  (kv-get [_ k name] (get-in @!state [k name]))
  (kv-put [_ k name v] (swap! !state assoc-in [k name] v))
  (kv-clear [_ k] (swap! !state dissoc k)))

(defn in-memory-keyed-state-store []
  (->InMemoryKeyedStateStore (atom {})))
