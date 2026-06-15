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
    (let [hot (r/hot-index)]
      (doseq [doc (get retrieval "kb")]
        (let [text (get doc "text")]
          (r/upsert hot (get doc "id") (r/embed text dim) text)))
      (r/two-tier hot nil 4 4))))

(defn- build-brain [path-name pspec dim chat-client]
  (if (= "llm" (get pspec "brain"))
    (llm/llm-brain (or chat-client (llm/stub-chat-client {:text "ok"}))
                   {:name path-name :system-prompt (get pspec "prompt" "")
                    :allowed-tools (get pspec "tools")
                    :max-iterations (as-int (get pspec "max_iterations") 6)})
    (brain/keyword-brain path-name {:tool-triggers (get pspec "tool_triggers") :dim dim})))

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
        reg (build-tools (get spec "tools"))
        path-specs (get agent "paths")
        paths (into {} (map (fn [[name pspec]]
                              [name {:name name :prompt (get pspec "prompt" "")
                                     :brain (build-brain name pspec dim chat-client)}])
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

(defn- stores-from-spec [spec]
  (let [conv (get-in spec ["stores" "conversation"])]
    (if (= "datomic" (get conv "kind"))
      (let [{:keys [conversation keyed]} (dat/datomic-stores
                                          {:db-name (or (get conv "db-name") "agentic")
                                           :storage-dir (or (get conv "storage-dir") :mem)})]
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
