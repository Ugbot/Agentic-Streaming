(ns agentic.mcp
  "MCP stdio server — exposes a tool registry over line-delimited JSON-RPC 2.0 (the same protocol the
   jagentic-core/tool-services ToolServers speak), so Clojure tools are callable by any MCP client.
   `handle` is a pure (registry, request) -> response for tests; `serve` runs the stdin/stdout loop."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [agentic.tools :as tools]
            [agentic.banking :as banking])
  (:import [java.io BufferedReader Writer]))

(def ^:private protocol-version "2024-11-05")

(defn- result [id v] {:jsonrpc "2.0" :id id :result v})
(defn- err [id code message] {:jsonrpc "2.0" :id id :error {:code code :message message}})

(defn handle
  "Dispatch a parsed JSON-RPC request map against the tool registry. Returns the response map, or nil
   for a notification (no :id)."
  [reg {:keys [id method params] :as _req}]
  (case method
    "initialize"
    (result id {:protocolVersion protocol-version
                :capabilities {:tools {}}
                :serverInfo {:name "agentic-clj" :version "0.1.0"}})

    ("notifications/initialized" "initialized") nil

    "ping" (result id {})

    "tools/list"
    (result id {:tools (mapv (fn [d] {:name (:name d) :description (:description d)
                                      :inputSchema (:input-schema d)})
                             (tools/tool-descriptors reg))})

    "tools/call"
    (let [name (get params "name") args (get params "arguments")]
      (try
        (let [out (tools/execute reg name args)]
          (result id {:content [{:type "text" :text (if (string? out) out (json/write-str out))}]
                      :isError false}))
        (catch Exception e
          (result id {:content [{:type "text" :text (str "error: " (.getMessage e))}]
                      :isError true}))))

    (if (nil? id) nil (err id -32601 (str "method not found: " method)))))

(defn- parse-request
  "JSON line -> a request map handle expects: keyword top-level keys, string-keyed :params (tools/call
   reads params by string key)."
  [line]
  (let [m (json/read-str line)]
    {:id (get m "id") :method (get m "method") :params (get m "params")}))

(defn serve
  "Run the JSON-RPC stdio loop. Reads one JSON object per line from `in`, writes one per line to `out`."
  ([reg] (serve reg *in* *out*))
  ([reg in out]
   (let [^BufferedReader rdr (io/reader in)
         ^Writer wtr (io/writer out)]
     (loop []
       (when-let [line (.readLine rdr)]
         (when-not (str/blank? line)
           (when-let [resp (try (handle reg (parse-request line)) (catch Exception _ nil))]
             (.write wtr (json/write-str resp))
             (.write wtr "\n")
             (.flush wtr)))
         (recur))))))

(defn -main [& _]
  (serve (agentic.banking/default-tools)))
