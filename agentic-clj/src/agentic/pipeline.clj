(ns agentic.pipeline
  "Declarative workflow loader: a v1 document (YAML, JSON or EDN) is validated by agentic.spec into
   the canonical kebab-keyword form and compiled into a runnable system — router, paths, verifier,
   guardrails, policies, saga, tools (constant / http / failing / MCP / A2A peers) and retrieval.
   `backend` must name a runtime this module can run; `stores` selects the storage engine, and an
   unreachable configured store fails the load unless it says `on_unavailable: degrade`."
  (:require [clojure.string :as str]
            [clj-http.client :as http]
            [clojure.data.json :as json]
            [agentic.spec :as spec]
            [agentic.tools :as tools]
            [agentic.mcp-client :as mcpc]
            [agentic.a2a :as a2a]
            [agentic.brain :as brain]
            [agentic.graph :as graph]
            [agentic.llm :as llm]
            [agentic.retrieval :as r]
            [agentic.guardrail :as guard]
            [agentic.cep :as cep]
            [agentic.cep-fold :as cep-fold]
            [agentic.core :as core]
            [agentic.store :as store]
            [agentic.log :as log]
            [agentic.store.datomic :as dat]))

(def backends
  "The `backend` values this module executes. Anything else is a configuration error, never a
   silent fallback."
  #{"local" "clojure" "datomic"})

(defn- as-int [x default] (cond (number? x) (int x) (string? x) (Integer/parseInt x) :else default))

;; ---- tools ----

(defn- failing-tool
  "`kind: failing` — raises on every call, or on the first `x-fail-attempts` calls when set, then
   returns `value`. The budget counts calls over the tool's life, across turns."
  [id budget value]
  (let [calls (atom 0)]
    (fn [_]
      (let [seen (swap! calls inc)]
        (if (or (nil? budget) (<= seen budget))
          (throw (ex-info (str id " failed") {:tool id :attempt seen}))
          value)))))

(defn- build-tools [tool-specs]
  (let [reg (tools/registry)]
    (doseq [{:keys [id kind description value url compensation] :as t :or {kind "constant"}} tool-specs]
      (let [desc (or description id)]
        (case kind
          "constant" (tools/register reg id desc (fn [_] value))
          "failing" (tools/register reg id desc (failing-tool id (:x-fail-attempts t) value))
          ("http" "agent") (tools/register reg id desc
                                           (fn [params]
                                             (-> (http/post url {:body (json/write-str (or params {}))
                                                                 :content-type :json :as :json})
                                                 :body)))
          (throw (spec/validation-error (str "unsupported tool kind " kind) ["tools" id "kind"])))
        (tools/tag reg id :kind kind)
        (when compensation (tools/tag reg id :compensation compensation))))
    reg))

(defn- register-mcp
  "For each `mcp:` server spawn a stdio client and register its discovered tools under `<name>_`."
  [reg mcp-specs]
  (doseq [m mcp-specs]
    (let [transport (get m "transport" "stdio")
          _ (when-not (= "stdio" transport)
              (throw (spec/validation-error (str "unsupported MCP transport " transport) ["mcp" (get m "name")])))
          raw-command (get m "command")
          args (get m "args")
          command (cond
                    (sequential? raw-command) (vec raw-command)
                    (some? args) (vec (cons raw-command args))
                    :else [raw-command])
          client (mcpc/mcp-client command (get m "env"))]
      (mcpc/register client reg (str (get m "name") "_"))))
  reg)

(defn- register-a2a
  "Register each `a2a:` peer as a delegating tool of kind `agent`, under its name (else id). Without a
   `url` the peer is a stand-in that answers `[<name>] delegated`, so a workflow can be exercised
   without the peer running."
  [reg a2a-specs]
  (doseq [{:keys [id name url description retries]} a2a-specs]
    (let [tool-id (or name id)]
      (tools/register reg tool-id (or description tool-id)
                      (if url
                        (a2a/peer-tool url (as-int retries 2))
                        (fn [_] (str "[" tool-id "] delegated"))))
      (tools/tag reg tool-id :kind "agent")))
  reg)

;; ---- retrieval ----

(defn- build-retriever [{:keys [kb vector-store] :as retrieval} dim]
  (when retrieval
    (let [idx (r/hot-index)]
      (doseq [{:keys [id text]} kb]
        (r/upsert idx id (r/embed text dim) text))
      ;; With a vector_store the KB is the cold tier (exact cosine KNN here) and the hot index stays
      ;; free for runtime upserts.
      (if vector-store
        (r/two-tier (r/hot-index) (fn [q k] (r/search idx q k)) 4 4)
        (r/two-tier idx nil 4 4)))))

;; ---- brains ----

(defn- build-chat-client
  "The ChatClient from `llm:`. provider: stub (deterministic, `script` replayed from the top on every
   turn) | ollama | openai."
  [{:keys [provider script base-url model api-key] :or {provider "stub"} :as llm-spec}]
  (when llm-spec
    (case provider
      "stub" (llm/scripted-chat-client script)
      "ollama" (llm/ollama-chat-client (cond-> {}
                                         base-url (assoc :base-url base-url)
                                         model (assoc :model model)))
      "openai" (llm/openai-chat-client (cond-> {}
                                         base-url (assoc :base-url base-url)
                                         model (assoc :model model)
                                         api-key (assoc :api-key api-key)))
      nil)))

(defn scripted-llm?
  "True when the workflow's `llm` section selects the spec's deterministic `stub` provider."
  [llm-spec]
  (and (some? llm-spec) (= "stub" (get llm-spec :provider "stub"))))

(defn- build-brain [path-name {:keys [brain prompt tools max-iterations tool-triggers threshold]} dim top-k chat-client context
                    & [{:keys [scripted?]}]]
  (if (= "llm" brain)
    (llm/llm-brain chat-client
                   {:name path-name :system-prompt (or prompt "")
                    :allowed-tools tools
                    ;; primitives.md section 8: the scripted reply is verbatim and a scripted call
                    ;; outside the path's tools is a validation error.
                    :verbatim-reply? (boolean scripted?)
                    :strict-tools? (boolean scripted?)
                    :max-iterations (as-int max-iterations 6)
                    :context-window (when context
                                      {:max-tokens (as-int (:max-tokens context) 512)
                                       :max-items (:max-items context)
                                       :compaction (or (:compaction context) "moscow")})})
    (brain/keyword-brain path-name (cond-> {:tool-triggers tool-triggers :dim dim :top-k top-k}
                                     threshold (assoc :threshold threshold)))))

(defn- apply-skills
  "Expand `skills: [name]` on each path: append the skill prompt, union its tools into the path's,
   and record its required facts."
  [paths skills]
  (let [by-name (into {} (map (juxt :name identity)) skills)]
    (into {}
          (map (fn [[pname pspec]]
                 (let [used (keep by-name (:skills pspec))
                       extra-prompt (str/join " " (keep :prompt used))
                       extra-tools (vec (mapcat :tools used))
                       facts (vec (mapcat :facts used))]
                   [pname (cond-> pspec
                            (seq extra-prompt) (update :prompt #(str/trim (str (or % "") " " extra-prompt)))
                            (seq extra-tools) (update :tools #(vec (distinct (concat % extra-tools))))
                            (seq facts) (assoc :facts facts))])))
          paths)))

(defn- build-router [{:keys [rules default]} fallback]
  (let [default (or default fallback)]
    (fn [event _ctx]
      (let [low (str/lower-case (or (:text event) ""))]
        (or (some (fn [[path keywords]]
                    (when (some #(str/includes? low (str/lower-case %)) keywords) path))
                  rules)
            default
            (throw (ex-info "no rule matched and router.default is not set" {:error/class :fatal})))))))

(defn- build-verifier
  "kind = prefix (default) | regex | none. `where` is the spec location for error messages:
   [\"agent\" \"verifier\"] or [\"agent\" \"paths\" name \"verifier\"]."
  [{:keys [kind pattern] :or {kind "prefix"}} where]
  (case kind
    "none" (fn [reply _] [true reply])
    "prefix" graph/prefix-verifier
    "regex" (do (when-not (string? pattern)
                  (throw (spec/validation-error "regex verifier needs a pattern" (conj where "pattern"))))
                (let [re (re-pattern pattern)]
                  (fn [reply _] [(boolean (and reply (re-find re reply))) reply])))
    (throw (spec/validation-error (str "unsupported verifier kind " kind) (conj where "kind")))))

(defn- build-guardrail [{:keys [kind stage deny reason lexicon blocked threshold]
                         :or {kind "regex" stage "input"}}]
  (let [outputs? (= "output" stage)
        rail (case kind
               "regex" (guard/regex-guardrail {:deny deny :reason (or reason "blocked by policy")
                                               :check-outputs outputs?})
               "classifier" (guard/classifier-guardrail {:lexicon lexicon :blocked blocked
                                                         :threshold (or threshold 0.5)
                                                         :reason (or reason "blocked by classifier policy")
                                                         :default-label "other"
                                                         :check-outputs outputs?})
               (throw (spec/validation-error (str "unsupported guardrail kind " kind) ["guardrails"])))]
    (cond-> rail (= "output" stage) (assoc :check-input nil))))

(defn build
  "Compile a workflow (any raw form, or already canonical) into {:graph :tools :retriever :workflow}."
  [document & [{:keys [chat-client]}]]
  (let [wf (spec/load-workflow document)
        _ (when-not (contains? backends (or (:backend wf) "local"))
            (throw (spec/validation-error (str "backend " (:backend wf) " is not a Clojure runtime; one of "
                                               (str/join ", " (sort backends)))
                                          ["backend"])))
        {:keys [agent retrieval embeddings context llm skills policies saga timers]} wf
        dim (or (:dim embeddings) (:dim retrieval) 256)
        top-k (or (:top-k retrieval) 4)
        cc (or chat-client (build-chat-client llm)
               (when (some #(= "llm" (:brain %)) (vals (:paths agent))) (llm/stub-chat-client {:text "ok"})))
        reg (-> (build-tools (:tools wf))
                (register-mcp (:mcp wf))
                (register-a2a (:a2a wf)))
        paths (apply-skills (:paths agent) skills)
        graph-paths (into {}
                          (map (fn [[name pspec]]
                                 [name (cond-> {:name name :prompt (or (:prompt pspec) "")
                                                :brain (build-brain name pspec dim top-k cc context
                                                                    {:scripted? (and (nil? chat-client) (scripted-llm? llm))})}
                                         (contains? pspec :x-suspend-until)
                                         (assoc :suspend-until (:x-suspend-until pspec))
                                         (some? (:verifier pspec))
                                         (assoc :verifier (build-verifier (:verifier pspec)
                                                                          ["agent" "paths" name "verifier"])))]))
                          paths)]
    {:graph {:router (build-router (:router agent) (first (keys paths)))
             :paths graph-paths
             :verifier (build-verifier (:verifier agent) ["agent" "verifier"])
             :guardrails (mapv build-guardrail (:guardrails wf))
             :policies policies
             :saga saga
             :context context
             :cep (cep-fold/compile-patterns (:cep wf))
             :timers (vec timers)
             :listeners []}
     :tools reg
     :retriever (build-retriever retrieval dim)
     :workflow wf}))

;; ---- stores (rule 7) ----

(defn- opt
  "A store option by wire name: canonical keyword for keys the schema declares, string otherwise."
  [m k]
  (let [kw (keyword (str/replace k "_" "-"))]
    (if (contains? m kw) (get m kw) (get m k))))

(defn- datomic-opts
  "Map a `stores.conversation` section onto datomic-stores opts (in-process, peer-server or cloud);
   only the keys present are forwarded."
  [conv]
  (let [g #(opt conv %)]
    (cond-> {:db-name (or (g "db-name") (g "db_name") "agentic")}
      (g "server-type")                (assoc :server-type (keyword (g "server-type")))
      (g "system")                     (assoc :system (g "system"))
      (g "storage-dir")                (assoc :storage-dir (g "storage-dir"))
      (g "endpoint")                   (assoc :endpoint (g "endpoint"))
      (g "access-key")                 (assoc :access-key (g "access-key"))
      (g "secret")                     (assoc :secret (g "secret"))
      (g "region")                     (assoc :region (g "region"))
      (some? (g "validate-hostnames")) (assoc :validate-hostnames (g "validate-hostnames"))
      (some? (g "create-database"))    (assoc :create-database? (g "create-database")))))

(defn- memory-stores []
  {:store (store/in-memory-conversation-store)
   :state (store/in-memory-keyed-state-store)
   :log (log/in-memory-event-log)})

(defn- store-unavailable [kind conv cause]
  (spec/validation-error (str "configured " kind " conversation store is unreachable: " (ex-message cause)
                              " (set stores.conversation.on_unavailable: degrade to run in memory)")
                         ["stores" "conversation"] cause))

(defn open-stores
  "The stores a workflow asks for. `kind: memory` (or no `stores`) is in-memory; `kind: datomic`
   opens Datomic and, when it cannot be reached, fails the load — or degrades to in-memory only when
   the section says `on_unavailable: degrade`. Any other kind is a validation error."
  [wf]
  (let [conv (get-in wf [:stores :conversation])
        kind (or (:kind conv) "memory")]
    (case kind
      "memory" (memory-stores)
      "datomic" (let [opened (try {:ok (dat/datomic-stores (datomic-opts conv))}
                                  (catch Exception e {:error e}))]
                  (if-let [{:keys [conversation keyed log]} (:ok opened)]
                    {:store conversation :state keyed :log log}
                    (if (= "degrade" (:on-unavailable conv))
                      (memory-stores)
                      (throw (store-unavailable kind conv (:error opened))))))
      (throw (spec/validation-error (str "unsupported conversation store kind " kind
                                         " for the Clojure runtime (memory, datomic)")
                                    ["stores" "conversation" "kind"])))))

;; ---- systems ----

(defn system-from
  "A runnable system from a raw or canonical workflow document plus opened stores. `stores` may also
   carry `:clock`, the processing clock workflow timers read (see agentic.core/local-system)."
  [document stores & [opts]]
  (let [{:keys [graph tools retriever workflow]} (build document opts)]
    (assoc (core/local-system graph tools retriever stores)
           :workflow workflow
           :cep (cep/compile-cep (cep-fold/without-tool-actions (:cep workflow))))))

(defn load-system
  "Load a workflow .yaml/.json/.edn into a runnable system, with the stores it configures. A
   declarative `cep:` section is compiled into wirings fed by `submit`."
  [path & [opts]]
  (let [wf (spec/load-path path)]
    (system-from wf (open-stores wf) opts)))

(defn submit
  "Process one turn, then feed the event to any compiled CEP wirings. Each wiring's action submits via
   the INNER core submit, which does not re-feed CEP, so CEP submits cannot recurse."
  [system event]
  (let [r (core/submit system event)]
    (doseq [w (:cep system)]
      (cep/cep-on-event w event #(core/submit system %) (:tools system)))
    r))
