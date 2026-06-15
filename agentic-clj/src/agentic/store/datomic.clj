(ns agentic.store.datomic
  "Datomic-backed storage — the first-class storage engine. Implements the agentic.store protocols
   over datomic.client.api against com.datomic/local (in-process: :mem for tests, a dir for dev; the
   same client API targets Datomic Pro/Cloud unchanged). Each message is an immutable datom, so the
   conversation transcript is a true event log — Datomic's history/as-of give time-travel for free."
  (:require [datomic.client.api :as d]
            [agentic.store :as store]))

(def schema
  [{:db/ident :conversation/id :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :conversation/user :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :message/conversation :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :message/role :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :message/content :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :message/tool-name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :message/position :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :attr/composite :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :attr/conversation :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :attr/key :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :attr/value :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :keyed/composite :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :keyed/value :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :fact/composite :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :fact/user :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :fact/key :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :fact/value :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :lt/conversation :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :lt/user :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :lt/role :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :lt/content :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :lt/position :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- msg-count [conn cid]
  (or (ffirst (d/q '[:find (count ?m) :in $ ?cid :where [?m :message/conversation ?cid]]
                   (d/db conn) cid))
      0))

(defn datomic-conversation-store [conn]
  (reify store/ConversationStore
    (append [_ cid msg]
      (d/transact conn {:tx-data [{:message/conversation cid
                                   :message/role (:role msg)
                                   :message/content (:content msg)
                                   :message/tool-name (or (:tool-name msg) "")
                                   :message/position (msg-count conn cid)}]}))
    (history [_ cid]
      (->> (d/q '[:find ?role ?content ?pos :in $ ?cid :where
                  [?m :message/conversation ?cid] [?m :message/role ?role]
                  [?m :message/content ?content] [?m :message/position ?pos]]
                (d/db conn) cid)
           (sort-by #(nth % 2))
           (mapv (fn [[role content _]] {:role role :content content}))))
    (message-count [_ cid] (msg-count conn cid))
    (put-attribute [_ cid k v]
      (d/transact conn {:tx-data [{:attr/composite (str cid "|" k) :attr/conversation cid
                                   :attr/key k :attr/value v}]}))
    (get-attribute [_ cid k]
      (ffirst (d/q '[:find ?v :in $ ?c :where [?a :attr/composite ?c] [?a :attr/value ?v]]
                   (d/db conn) (str cid "|" k))))
    (attributes [_ cid]
      (into {} (d/q '[:find ?k ?v :in $ ?cid :where
                      [?a :attr/conversation ?cid] [?a :attr/key ?k] [?a :attr/value ?v]]
                    (d/db conn) cid)))
    (associate-user [_ cid uid]
      (d/transact conn {:tx-data [{:conversation/id cid :conversation/user uid}]}))
    (conversations-for-user [_ uid]
      (->> (d/q '[:find ?cid :in $ ?uid :where
                  [?e :conversation/user ?uid] [?e :conversation/id ?cid]]
                (d/db conn) uid)
           (map first) vec))
    (clear-conversation [_ cid]
      (let [db (d/db conn)
            ents (concat
                  (map first (d/q '[:find ?m :in $ ?cid :where [?m :message/conversation ?cid]] db cid))
                  (map first (d/q '[:find ?a :in $ ?cid :where [?a :attr/conversation ?cid]] db cid))
                  (map first (d/q '[:find ?e :in $ ?cid :where [?e :conversation/id ?cid]] db cid)))]
        (when (seq ents)
          (d/transact conn {:tx-data (mapv (fn [e] [:db/retractEntity e]) ents)}))))))

(defn datomic-keyed-state-store [conn]
  (reify store/KeyedStateStore
    (kv-get [_ k name]
      (ffirst (d/q '[:find ?v :in $ ?c :where [?e :keyed/composite ?c] [?e :keyed/value ?v]]
                   (d/db conn) (str k "|" name))))
    (kv-put [_ k name v]
      (d/transact conn {:tx-data [{:keyed/composite (str k "|" name) :keyed/value (str v)}]}))
    (kv-clear [_ k]
      (let [ents (map first (d/q '[:find ?e :in $ ?pre :where
                                   [?e :keyed/composite ?c] [(clojure.string/starts-with? ?c ?pre)]]
                                 (d/db conn) (str k "|")))]
        (when (seq ents)
          (d/transact conn {:tx-data (mapv (fn [e] [:db/retractEntity e]) ents)}))))))

(defn datomic-long-term-store [conn]
  (reify store/LongTermStore
    (save-turn [_ cid uid role content]
      (let [pos (or (ffirst (d/q '[:find (count ?m) :in $ ?cid :where [?m :lt/conversation ?cid]]
                                 (d/db conn) cid)) 0)]
        (d/transact conn {:tx-data [{:lt/conversation cid :lt/user uid :lt/role role
                                     :lt/content content :lt/position pos}]})))
    (load-history [_ cid]
      (->> (d/q '[:find ?role ?content ?pos :in $ ?cid :where
                  [?m :lt/conversation ?cid] [?m :lt/role ?role]
                  [?m :lt/content ?content] [?m :lt/position ?pos]]
                (d/db conn) cid)
           (sort-by #(nth % 2))
           (mapv (fn [[role content _]] [role content]))))
    (save-fact [_ uid k v]
      (d/transact conn {:tx-data [{:fact/composite (str uid "|" k) :fact/user uid :fact/key k :fact/value v}]}))
    (facts [_ uid]
      (into {} (d/q '[:find ?k ?v :in $ ?uid :where
                      [?f :fact/user ?uid] [?f :fact/key ?k] [?f :fact/value ?v]]
                    (d/db conn) uid)))
    (lt-conversations-for-user [_ uid]
      (->> (d/q '[:find ?cid :in $ ?uid :where [?m :lt/user ?uid] [?m :lt/conversation ?cid]]
                (d/db conn) uid)
           (map first) distinct vec))))

(defn datomic-stores
  "Create in-process Datomic stores. opts: {:system :db-name :storage-dir} (storage-dir :mem for
   tests, a path string for dev). Returns {:conn :conversation :keyed :long-term}."
  ([] (datomic-stores {}))
  ([{:keys [system db-name storage-dir] :or {system "agentic" db-name "agentic" storage-dir :mem}}]
   (let [client (d/client {:server-type :datomic-local :system system :storage-dir storage-dir})]
     (d/create-database client {:db-name db-name})
     (let [conn (d/connect client {:db-name db-name})]
       (d/transact conn {:tx-data schema})
       {:conn conn
        :conversation (datomic-conversation-store conn)
        :keyed (datomic-keyed-state-store conn)
        :long-term (datomic-long-term-store conn)}))))
