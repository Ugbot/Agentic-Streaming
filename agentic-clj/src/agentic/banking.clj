(ns agentic.banking
  "The banking router→path→verifier worked example — the Clojure mirror of jagentic-core Banking /
   pyagentic.banking, reproducing the exact KB, router rules, rule-brain replies and tools so the
   cross-core parity goldens hold."
  (:require [clojure.string :as str]
            [agentic.tools :as tools]
            [agentic.retrieval :as r]
            [agentic.context :as ctx]))

(def dim 256)

(def kb
  {"kb_cards_types" "We offer three card types: classic, gold, and platinum, each with different fees."
   "kb_cards_crypto" "Crypto cash-back can be redeemed to a linked wallet or a manual address."
   "kb_payments_limits" "Daily transfer limits are 10,000 by default; raise them in settings."
   "kb_payments_dispute" "To dispute a charge, open the transaction and tap Dispute within 60 days."})

(defn rule-brain
  "Keyword + tool/retrieval rule brain (matches jagentic-core Banking.RuleBrain exactly)."
  [name]
  (fn [user-text context]
    (let [low (str/lower-case user-text)]
      (cond
        (str/includes? low "balance")
        (let [bal (ctx/call-tool context "get_balance" {"user" (:user-id context)})]
          (str "[" name "] Your balance is " bal "."))

        (:retriever context)
        (let [hits (r/retrieve (:retriever context) (r/embed user-text dim) 1)]
          (if (and (seq hits) (> (:score (first hits)) 0.15))
            (str "[" name "] " (:text (first hits)))
            (str "[" name "] I can help with " name " questions. You said: \"" user-text "\"")))

        :else
        (str "[" name "] I can help with " name " questions. You said: \"" user-text "\"")))))

(defn router [event _ctx]
  (let [low (str/lower-case (:text event))]
    (cond
      (or (str/includes? low "card") (str/includes? low "crypto")
          (str/includes? low "cash-back") (str/includes? low "cashback")) "cards"
      (or (str/includes? low "transfer") (str/includes? low "payment") (str/includes? low "dispute")
          (str/includes? low "charge") (str/includes? low "limit") (str/includes? low "balance")) "payments"
      :else "general")))

(defn default-tools []
  (let [reg (tools/registry)]
    (tools/register reg "get_balance" "Look up the user's balance" (fn [_] 1234.56))
    reg))

(defn retriever []
  (let [hot (r/hot-index)]
    (doseq [[id text] kb]
      (r/upsert hot id (r/embed text dim) text))
    (r/two-tier hot nil 4 4)))

(defn build-graph []
  {:router router
   :paths {"cards" {:name "cards" :prompt "You answer card questions." :brain (rule-brain "cards")}
           "payments" {:name "payments" :prompt "You answer payment questions." :brain (rule-brain "payments")}
           "general" {:name "general" :prompt "You answer general questions." :brain (rule-brain "general")}}
   :verifier (fn [reply _ctx] [(boolean (and reply (str/starts-with? reply "["))) reply])
   :guardrails []
   :listeners []})
