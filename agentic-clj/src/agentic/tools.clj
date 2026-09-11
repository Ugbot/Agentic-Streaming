(ns agentic.tools
  "Tool registry — named {params -> result} fns with an optional input JSON-schema. An atom of
   id -> {:description :schema :fn}. Mirror of jagentic-core ToolRegistry.")

(defn registry [] (atom {}))

(defn register
  "Register a tool; returns the registry (chainable)."
  ([reg id description f] (register reg id description nil f))
  ([reg id description schema f]
   (swap! reg assoc id {:description description :schema schema :fn f})
   reg))

(defn tag
  "Attach metadata to a registered tool (its spec `:kind`, a saga `:compensation` tool id, ...)."
  [reg id k v]
  (swap! reg assoc-in [id k] v)
  reg)

(defn kind [reg id] (get-in @reg [id :kind] "constant"))

(defn compensation [reg id] (get-in @reg [id :compensation]))

(defn ids [reg] (vec (keys @reg)))

(defn execute [reg id params]
  (if-let [t (get @reg id)]
    ((:fn t) (or params {}))
    (throw (ex-info (str "no such tool: " id) {:id id}))))

(defn specs
  "[{:name :description}] — what an LLM brain shows the model."
  [reg]
  (mapv (fn [[id t]] {:name id :description (:description t)}) @reg))

(defn tool-descriptors
  "[{:name :description :input-schema}] — the MCP tools/list shape."
  [reg]
  (mapv (fn [[id t]] {:name id :description (:description t)
                      :input-schema (or (:schema t) {:type "object"})}) @reg))
