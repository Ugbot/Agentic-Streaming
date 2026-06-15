(ns agentic.mcp-client
  "MCP stdio client — connect to a Model Context Protocol server over newline-delimited JSON-RPC 2.0
   and surface its tools in a `ToolRegistry`, so an agent can call external MCP tool servers. The
   Clojure mirror of jagentic-core's McpStdioClient and pyagentic's McpClient. No third-party MCP
   dependency — just a BufferedReader/Writer pair (real process via java.lang.ProcessBuilder, or any
   piped streams for tests).

   Protocol: `initialize` -> notification `notifications/initialized` -> `tools/list` -> `tools/call`.
   A `tools/call` result's text is the join (with \"\\n\") of its `content` blocks where type==\"text\"."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [agentic.tools :as tools])
  (:import [java.io BufferedReader Writer InputStreamReader OutputStreamWriter]
           [java.lang ProcessBuilder ProcessBuilder$Redirect]
           [java.nio.charset StandardCharsets]))

(def ^:private protocol-version "2024-11-05")

(defrecord McpClient [^BufferedReader reader ^Writer writer counter lock process specs])

(defn- write-line [^Writer wtr ^String json-str]
  (.write wtr json-str)
  (.write wtr "\n")
  (.flush wtr))

(defn- notification [client method params]
  (write-line (:writer client)
              (json/write-str {:jsonrpc "2.0" :method method :params (or params {})})))

(defn- request
  "Write a JSON-RPC request and block until the response with a matching id arrives, skipping
   notifications and non-JSON lines. Throws on a JSON-RPC error or premature EOF. Synchronized so
   concurrent callers don't interleave their reads."
  [client method params]
  (locking (:lock client)
    (let [id (swap! (:counter client) inc)
          ^Writer wtr (:writer client)
          ^BufferedReader rdr (:reader client)]
      (write-line wtr (json/write-str {:jsonrpc "2.0" :id id :method method :params (or params {})}))
      (loop []
        (let [line (.readLine rdr)]
          (when (nil? line)
            (throw (ex-info (str "MCP server closed before responding to " method) {:method method})))
          (if (str/blank? line)
            (recur)
            (let [node (try (json/read-str line) (catch Exception _ ::skip))]
              (cond
                (= node ::skip) (recur)
                (= (get node "id") id)
                (if-let [error (get node "error")]
                  (throw (ex-info (str "MCP error: " (json/write-str error)) {:error error :method method}))
                  node)
                :else (recur)))))))))

(defn- discover-specs
  "Run the initialize handshake and return the [{:name :description}] from tools/list."
  [client]
  (request client "initialize"
           {:protocolVersion protocol-version
            :capabilities {}
            :clientInfo {:name "agentic-clj" :version "0.1.0"}})
  (notification client "notifications/initialized" {})
  (let [listed (request client "tools/list" {})
        tools (get-in listed ["result" "tools"])]
    (vec (for [t (if (sequential? tools) tools [])]
           {:name (get t "name" "") :description (get t "description" "")}))))

(defn mcp-client-from-streams
  "Build a client over an already-open reader/writer pair and run the initialize handshake. The
   testable core constructor — works with any java.io.BufferedReader + java.io.Writer."
  [^BufferedReader reader ^Writer writer]
  (let [client (->McpClient reader writer (atom 0) (Object.) nil (atom []))
        specs (discover-specs client)]
    (reset! (:specs client) specs)
    client))

(defn mcp-client
  "Spawn an MCP server process via java.lang.ProcessBuilder (command is a vector like
   [\"python\" \"-m\" \"server\"]), wire its stdin/stdout, and run the handshake. Returns a client.
   `env` (optional map) is merged into the child environment. stderr is left to the server (not
   merged into stdout, so it can't corrupt the JSON-RPC stream)."
  ([command] (mcp-client command nil))
  ([command env]
   (let [^java.util.List cmd (vec (map str command))
         pb (ProcessBuilder. cmd)]
     (.redirectErrorStream pb false)
     (when (seq env)
       (let [pe (.environment pb)]
         (doseq [[k v] env] (.put pe (str k) (str v)))))
     (let [process (.start pb)
           writer (OutputStreamWriter. (.getOutputStream process) StandardCharsets/UTF_8)
           reader (BufferedReader. (InputStreamReader. (.getInputStream process) StandardCharsets/UTF_8))
           base (mcp-client-from-streams reader writer)]
       (assoc base :process process)))))

(defn list-tools
  "The discovered tool specs: [{:name :description}]."
  [client]
  @(:specs client))

(defn call-tool
  "Invoke an MCP tool and return the joined text of its result `content` text blocks."
  [client name args]
  (let [resp (request client "tools/call" {:name name :arguments (or args {})})
        content (get-in resp ["result" "content"])]
    (->> (if (sequential? content) content [])
         (filter #(= "text" (get % "type")))
         (map #(get % "text" ""))
         (str/join "\n"))))

(defn register
  "Register every discovered MCP tool into `reg` (ids `(str prefix name)`); returns `reg`."
  [client reg prefix]
  (let [p (or prefix "")]
    (doseq [{:keys [name description]} (list-tools client)]
      (tools/register reg (str p name) description
                      (fn [params] (call-tool client name params)))))
  reg)

(defn close
  "Best-effort shutdown: close the writer and, if a process was spawned, destroy + await it."
  [client]
  (try (.close ^Writer (:writer client)) (catch Exception _ nil))
  (when-let [^Process p (:process client)]
    (.destroy p)
    (try (.waitFor p) (catch InterruptedException _ (.interrupt (Thread/currentThread))))))
