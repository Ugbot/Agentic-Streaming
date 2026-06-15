(ns agentic.pipeline
  "Declarative pipeline loader — the same schema as the Python/Go/Java/Pekko loaders, as EDN (native)
   or YAML (clj-yaml, parsed with string keys to match the shared docs). build → {:graph :tools
   :retriever}; load → a runnable system. Datomic-backed stores selectable via the stores section."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clj-yaml.core :as yaml]
            [clj-http.client :as http]
            [clojure.data.json :as json]
            [agentic.tools :as tools]
            [agentic.brain :as brain]
            [agentic.llm :as llm]
            [agentic.retrieval :as r]
            [agentic.guardrail :as guard]
            [agentic.core :as core]
            [agentic.store :as store]
            [agentic.store.datomic :as dat]))

(defn- as-int [x default] (cond (number? x) (int x) (string? x) (Integer/parseInt x) :else default))

(defn- build-tools [tool-specs]
  (let [reg (tools/registry)]
    (doseq [t tool-specs]
      (let [id (get t "id") kind (get t "kind" "constant") desc (get t "description" id)]
        (case kind
          "constant" (let [v (get t "value")] (tools/register reg id desc (fn [_] v)))
          ("http" "agent") (let [url (get t "url")]
                             (tools/register reg id desc
                                             (fn [params]
                                               (-> (http/post url {:body (json/write-str (or params {}))
                                                                   :content-type :json :as :json})
                                                   :body))))
          (throw (ex-info (str "unknown tool kind " kind) {:id id})))))
    reg))

(defn- build-retriever [retrieval dim]
  (when retrieval
    (let [kb (get retrieval "kb")
          vector-store (get retrieval "vector_store")
          idx (r/hot-index)]
      (doseq [doc kb]
        (let [text (get doc "text")]
          (r/upsert idx (get doc "id") (r/embed text dim) text)))
      ;; When a vector_store (cold tier) is declared, the KB lives there and the hot index stays
      ;; empty for runtime working-memory upserts — mirroring the cores. Clojure's cold tier is exact
      ;; cosine KNN (a correctness-superset of HNSW ANN); the matrix records that nuance.
      (if vector-store
        (r/two-tier (r/hot-index) (fn [q k] (r/search idx q k)) 4 4)
        (r/two-tier idx nil 4 4)))))

(defn- build-chat-client
  "Build the ChatClient from the spec's `llm:` section. provider: stub (deterministic, from `script`)
   | ollama | openai. Returns nil when no `llm:` section (build-brain then uses an 'ok' stub)."
  [llm-spec]
  (when llm-spec
    (case (get llm-spec "provider" "stub")
      "stub" (apply llm/stub-chat-client
                    (mapv (fn [step]
                            (if (contains? step "tool")
                              {:tool (get step "tool") :args (or (get step "args") {})}
                              {:text (get step "text" "ok")}))
                          (get llm-spec "script")))
      "ollama" (llm/ollama-chat-client (cond-> {}
                                         (get llm-spec "base_url") (assoc :base-url (get llm-spec "base_url"))
                                         (get llm-spec "model") (assoc :model (get llm-spec "model"))))
      "openai" (llm/openai-chat-client (cond-> {}
                                         (get llm-spec "base_url") (assoc :base-url (get llm-spec "base_url"))
                                         (get llm-spec "model") (assoc :model (get llm-spec "model"))
                                         (get llm-spec "api_key") (assoc :api-key (get llm-spec "api_key"))))
      nil)))

(defn- build-brain [path-name pspec dim chat-client context]
  (if (= "llm" (get pspec "brain"))
    (llm/llm-brain (or chat-client (llm/stub-chat-client {:text "ok"}))
                   {:name path-name :system-prompt (get pspec "prompt" "")
                    :allowed-tools (get pspec "tools")
                    :max-iterations (as-int (get pspec "max_iterations") 6)
                    :context-window (when context
                                      {:max-tokens (as-int (get context "max_tokens") 512)
                                       :compaction (get context "compaction" "moscow")})})
    (brain/keyword-brain path-name {:tool-triggers (get pspec "tool_triggers") :dim dim})))

(defn- apply-skills
  "Expand `skills: [name]` on each path: append the skill prompt fragment, union the skill's tools
   into the path's allowed tools, and record required facts (`_facts`). Mirrors the cores — for the
   rule brain prompts/facts are inert, but the declaration is honoured (an LLM brain sees the tools)."
  [path-specs skills-spec]
  (let [by-name (into {} (map (fn [s] [(get s "name") s]) skills-spec))]
    (into {}
          (map (fn [[pname pspec]]
                 (let [skills (keep by-name (get pspec "skills"))
                       extra-prompt (str/join " " (keep #(get % "prompt") skills))
                       extra-tools (vec (mapcat #(get % "tools") skills))
                       facts (vec (mapcat #(get % "facts") skills))]
                   [pname (cond-> pspec
                            (seq extra-prompt) (update "prompt" #(str/trim (str (or % "") " " extra-prompt)))
                            (seq extra-tools)  (update "tools" #(vec (distinct (concat % extra-tools))))
                            (seq facts)        (assoc "_facts" facts))]))
               path-specs))))

(defn- build-router [router-spec default-path]
  (let [default (get router-spec "default" default-path)
        rules (get router-spec "rules")]
    (fn [event _ctx]
      (let [low (str/lower-case (:text event))]
        (or (some (fn [[path keywords]]
                    (when (some #(str/includes? low (str/lower-case %)) keywords) path))
                  rules)
            default)))))

(defn- build-verifier [vspec]
  (if (= "none" (get vspec "kind"))
    (fn [reply _] [true reply])
    (fn [reply _] [(boolean (and reply (str/starts-with? reply "["))) reply])))

(defn- build-guardrail [g]
  (case (get g "kind" "regex")
    "regex" (guard/regex-guardrail {:deny (get g "deny") :reason (get g "reason" "blocked by policy")
                                    :check-outputs (get g "check_outputs")})
    "classifier" (guard/classifier-guardrail {:lexicon (get g "lexicon") :blocked (get g "blocked")
                                              :threshold (or (get g "threshold") 0.5)
                                              :reason (get g "reason" "blocked by classifier policy")
                                              :default-label (get g "default_label" "other")
                                              :check-outputs (get g "check_outputs")})
    (throw (ex-info (str "unknown guardrail kind " (get g "kind")) {}))))

(defn build
  "Compile a spec map (string keys, like the shared YAML/EDN) into {:graph :tools :retriever}."
  [spec & [{:keys [chat-client]}]]
  (let [agent (get spec "agent")
        retrieval (get spec "retrieval")
        dim (as-int (get retrieval "dim") 256)
        retriever (build-retriever retrieval dim)
        context (get spec "context")
        cc (or chat-client (build-chat-client (get spec "llm")))
        reg (build-tools (get spec "tools"))
        path-specs (apply-skills (get agent "paths") (get spec "skills"))
        paths (into {} (map (fn [[name pspec]]
                              [name {:name name :prompt (get pspec "prompt" "")
                                     :brain (build-brain name pspec dim cc context)}])
                            path-specs))
        default-path (or (get-in agent ["router" "default"]) (first (keys path-specs)))
        router (build-router (get agent "router") default-path)
        verifier (build-verifier (get agent "verifier"))
        guardrails (mapv build-guardrail (get spec "guardrails"))]
    {:graph {:router router :paths paths :verifier verifier :guardrails guardrails :listeners []}
     :tools reg :retriever retriever}))

(defn- read-spec [path]
  (let [content (slurp path)]
    (if (str/ends-with? path ".edn")
      (edn/read-string content)
      (yaml/parse-string content :keywords false))))

(defn- datomic-opts
  "Map a YAML/EDN stores.conversation section (string keys) onto datomic-stores opts. Supports the
   in-process and the external (peer-server / cloud) deployments — only the keys present are forwarded."
  [conv]
  (let [g #(get conv %)]
    (cond-> {:db-name (or (g "db-name") "agentic")}
      (g "server-type")              (assoc :server-type (keyword (g "server-type")))
      (g "system")                   (assoc :system (g "system"))
      (g "storage-dir")              (assoc :storage-dir (g "storage-dir"))
      (g "endpoint")                 (assoc :endpoint (g "endpoint"))
      (g "access-key")               (assoc :access-key (g "access-key"))
      (g "secret")                   (assoc :secret (g "secret"))
      (g "region")                   (assoc :region (g "region"))
      (some? (g "validate-hostnames")) (assoc :validate-hostnames (g "validate-hostnames"))
      (some? (g "create-database"))    (assoc :create-database? (g "create-database")))))

(defn- stores-from-spec [spec]
  (let [conv (get-in spec ["stores" "conversation"])]
    (if (= "datomic" (get conv "kind"))
      (let [{:keys [conversation keyed]} (dat/datomic-stores (datomic-opts conv))]
        [conversation keyed])
      [(store/in-memory-conversation-store) (store/in-memory-keyed-state-store)])))

(defn load-system
  "Load a pipeline.yaml/.edn into a runnable system."
  [path & [opts]]
  (let [spec (read-spec path)
        {:keys [graph tools retriever]} (build spec opts)
        [conv keyed] (stores-from-spec spec)]
    (core/local-system graph tools retriever conv keyed)))

(defn submit [system event] (core/submit system event))
