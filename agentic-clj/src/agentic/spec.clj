(ns agentic.spec
  "Loading the agentic/v1 workflow IR. A document is read from YAML, JSON or EDN (kebab-case keywords,
   per the mapping in spec/README.md), canonicalized, validated against spec/v1/workflow.schema.json,
   filled with the schema's defaults, and checked against the seven rules the schema cannot express.
   Every problem is a `validation` error carrying the offending key path.

   The schemas are read from the repository's `spec/` directory (found by walking up from the working
   directory, or via the AGENTIC_SPEC_DIR environment variable) — the Clojure runtime never ships its
   own copy, so the schema, the fixtures and this loader version together."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clj-yaml.core :as yaml]
            [agentic.spec.schema :as schema])
  (:import [java.io File]))

(def spec-version "agentic/v1")

(defn validation-error
  "An ex-info of error class :validation. `path` is the wire key path (vector) of the offending key."
  ([message path] (validation-error message path nil))
  ([message path cause]
   (ex-info message {:error/class :validation :path (vec path) :message message} cause)))

(defn validation-error? [t]
  (= :validation (:error/class (ex-data t))))

;; ---- locating spec/ ----

(defn- has-spec? [^File dir]
  (.isFile (io/file dir "spec" "v1" "workflow.schema.json")))

(defn spec-root
  "The repository `spec/` directory: AGENTIC_SPEC_DIR when set, else the nearest ancestor of the
   working directory that contains spec/v1/workflow.schema.json."
  []
  (if-let [env (System/getenv "AGENTIC_SPEC_DIR")]
    (let [f (io/file env)]
      (when-not (.isFile (io/file f "v1" "workflow.schema.json"))
        (throw (ex-info (str "AGENTIC_SPEC_DIR does not contain v1/workflow.schema.json: " env) {:dir env})))
      f)
    (loop [dir (.getAbsoluteFile (io/file (System/getProperty "user.dir")))]
      (cond
        (nil? dir) (throw (ex-info "spec/ directory not found; set AGENTIC_SPEC_DIR" {}))
        (has-spec? dir) (io/file dir "spec")
        :else (recur (.getParentFile dir))))))

(defn- read-json-file [f] (json/read-str (slurp f)))

(def workflow-schema
  (delay (read-json-file (io/file (spec-root) "v1" "workflow.schema.json"))))

(def result-schema
  (delay (read-json-file (io/file (spec-root) "v1" "result.schema.json"))))

;; ---- reading documents ----

(defn- yaml->data
  "clj-yaml returns ordered maps and lazy seqs; make the shape plain so it compares structurally."
  [v]
  (cond (map? v) (into {} (map (fn [[k v]] [(if (keyword? k) (name k) (str k)) (yaml->data v)])) v)
        (sequential? v) (mapv yaml->data v)
        :else v))

(defn read-document
  "Parse a workflow file by extension: .yaml/.yml, .json, or .edn. Returns the raw data."
  [path]
  (let [content (slurp path)
        p (str/lower-case (str path))]
    (cond
      (str/ends-with? p ".edn") (edn/read-string content)
      (str/ends-with? p ".json") (json/read-str content)
      :else (yaml->data (yaml/parse-string content :keywords false)))))

(defn canonical
  "Any raw workflow data (string or keyword keys, snake or kebab) to the canonical kebab-keyword form."
  [raw]
  (schema/canonicalize @workflow-schema raw))

(def ->wire
  "Canonical form to the wire form (string snake_case keys) every other runtime reads."
  schema/->wire)

(defn ->edn-string
  "Render a workflow in its EDN spelling."
  [workflow]
  (pr-str (canonical workflow)))

;; ---- the seven validation rules ----

(defn- fail [problems message path] (conj problems {:path path :message message}))

(defn tool-ids
  "Every tool id the document registers statically: `tools` and `a2a` peers (by name, else id). MCP
   tools are discovered from the server at build time and are registered under `<server name>_`, so a
   reference is accepted when it carries a declared server's prefix."
  [wf]
  (concat (map :id (:tools wf))
          (map #(or (:name %) (:id %)) (:a2a wf))))

(defn- mcp-prefixes [wf] (map #(str (get % "name") "_") (:mcp wf)))

(defn- registered? [wf ids id]
  (or (contains? ids id)
      (some #(str/starts-with? id %) (mcp-prefixes wf))))

(defn rule-problems
  "Rules 1–6 of spec/README.md over a canonical workflow. Rule 7 (unreachable stores) is enforced where
   the stores are opened, see agentic.pipeline. Returns [{:path :message}]."
  [wf]
  (let [paths (get-in wf [:agent :paths] {})
        router (get-in wf [:agent :router] {})
        ids (set (tool-ids wf))
        skills (into {} (map (juxt :name identity)) (:skills wf))
        llm? (contains? wf :llm)]
    (-> []
        ;; 1. router targets exist
        (as-> p (reduce (fn [p path]
                          (if (contains? paths path) p
                              (fail p (str "router rule names unknown path " path) ["agent" "router" "rules" path])))
                        p (keys (:rules router))))
        (as-> p (let [d (:default router)]
                  (if (or (nil? d) (contains? paths d)) p
                      (fail p (str "router default names unknown path " d) ["agent" "router" "default"]))))
        ;; 2. + 4. per-path references
        (as-> p (reduce
                 (fn [p [pname spec]]
                   (-> p
                       (as-> p (reduce (fn [p t] (if (registered? wf ids t) p
                                                     (fail p (str "unknown tool " t) ["agent" "paths" pname "tools"])))
                                       p (:tools spec)))
                       (as-> p (reduce (fn [p [kw t]] (if (registered? wf ids t) p
                                                          (fail p (str "unknown tool " t) ["agent" "paths" pname "tool_triggers" kw])))
                                       p (:tool-triggers spec)))
                       (as-> p (reduce (fn [p s] (if (contains? skills s) p
                                                     (fail p (str "unknown skill " s) ["agent" "paths" pname "skills"])))
                                       p (:skills spec)))
                       (as-> p (if (and (= "llm" (:brain spec)) (not llm?))
                                 (fail p "brain: llm requires an llm block" ["agent" "paths" pname "brain"])
                                 p))))
                 p paths))
        (as-> p (reduce (fn [p [sname skill]]
                          (reduce (fn [p t] (if (registered? wf ids t) p
                                                (fail p (str "unknown tool " t) ["skills" sname "tools"])))
                                  p (:tools skill)))
                        p skills))
        ;; 3. tool ids unique
        (as-> p (reduce (fn [p [id n]] (if (> n 1) (fail p (str "duplicate tool id " id) ["tools" id]) p))
                        p (frequencies (tool-ids wf))))
        ;; 5. dims agree
        (as-> p (let [r (get-in wf [:retrieval :dim]) e (get-in wf [:embeddings :dim])]
                  (if (and r e (not= r e))
                    (fail p (str "retrieval.dim " r " disagrees with embeddings.dim " e) ["retrieval" "dim"])
                    p)))
        ;; 2. + 6. saga steps
        (as-> p (reduce (fn [p [i step]]
                          (-> p
                              (as-> p (if (registered? wf ids (:tool step)) p
                                          (fail p (str "unknown tool " (:tool step)) ["saga" "steps" i "tool"])))
                              (as-> p (let [c (:compensate-with step)]
                                        (if (or (nil? c) (registered? wf ids c)) p
                                            (fail p (str "compensate_with names unknown tool " c) ["saga" "steps" i "compensate_with"]))))))
                        p (map-indexed vector (get-in wf [:saga :steps])))))))

(defn- check-version [raw]
  (let [v (some (fn [k] (get raw k)) ["spec_version" :spec_version :spec-version "spec-version"])]
    (when (and (some? v) (not= (name v) spec-version))
      (throw (validation-error (str "/spec_version: unsupported spec_version " (name v) "; this runtime reads " spec-version)
                               ["spec_version"])))))

(defn load-workflow
  "Raw document data → validated canonical workflow with defaults applied. Throws a :validation
   ex-info on the first problem class found (schema, then the rules)."
  [raw]
  (when-not (map? raw)
    (throw (validation-error "a workflow document must be a mapping" [])))
  (check-version raw)
  (let [root @workflow-schema
        wf (canonical raw)
        wire (->wire wf)
        problems (schema/errors root wire)]
    (when (seq problems)
      (throw (validation-error (str/join "; " (map schema/explain problems)) (:path (first problems)))))
    (let [wf (schema/canonicalize root (schema/apply-defaults root wire))
          rules (rule-problems wf)]
      (when (seq rules)
        (throw (validation-error (str/join "; " (map schema/explain rules)) (:path (first rules)))))
      wf)))

(defn load-path
  "Read and validate a workflow document from a .yaml/.json/.edn path."
  [path]
  (load-workflow (read-document path)))

;; ---- normalized results ----

(defn result-problems
  "Validation problems of a wire-form normalized result against spec/v1/result.schema.json."
  [result]
  (schema/errors @result-schema result))
