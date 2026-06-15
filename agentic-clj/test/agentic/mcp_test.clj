(ns agentic.mcp-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [agentic.mcp :as mcp]
            [agentic.tools :as tools]
            [agentic.banking :as banking])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(deftest handle-dispatches-jsonrpc
  (let [reg (banking/default-tools)]
    (testing "initialize"
      (let [r (mcp/handle reg {:id 1 :method "initialize" :params {}})]
        (is (= "2.0" (:jsonrpc r)))
        (is (= "agentic-clj" (get-in r [:result :serverInfo :name])))))
    (testing "tools/list exposes registered tools"
      (let [r (mcp/handle reg {:id 2 :method "tools/list" :params {}})
            names (set (map :name (get-in r [:result :tools])))]
        (is (contains? names "get_balance"))))
    (testing "tools/call executes a tool"
      (let [r (mcp/handle reg {:id 3 :method "tools/call"
                               :params {"name" "get_balance" "arguments" {}}})]
        (is (false? (get-in r [:result :isError])))
        (is (re-find #"1234\.56" (get-in r [:result :content 0 :text])))))
    (testing "unknown tool surfaces an error result, not a crash"
      (let [r (mcp/handle reg {:id 4 :method "tools/call" :params {"name" "nope" "arguments" {}}})]
        (is (true? (get-in r [:result :isError])))))
    (testing "notifications return nil (no response)"
      (is (nil? (mcp/handle reg {:method "notifications/initialized"}))))
    (testing "unknown method -> JSON-RPC error"
      (is (= -32601 (get-in (mcp/handle reg {:id 5 :method "bogus"}) [:error :code]))))))

(deftest serve-stdio-roundtrip
  (let [reg (banking/default-tools)
        in (str (json/write-str {:jsonrpc "2.0" :id 1 :method "initialize" :params {}}) "\n"
                (json/write-str {:jsonrpc "2.0" :method "notifications/initialized"}) "\n"
                (json/write-str {:jsonrpc "2.0" :id 2 :method "tools/call"
                                 :params {:name "get_balance" :arguments {}}}) "\n")
        out (ByteArrayOutputStream.)]
    (mcp/serve reg (ByteArrayInputStream. (.getBytes in)) out)
    (let [lines (remove clojure.string/blank? (clojure.string/split-lines (str out)))
          msgs (map #(json/read-str % :key-fn keyword) lines)]
      ;; two responses: initialize + tools/call (the notification produces none)
      (is (= 2 (count msgs)))
      (is (= "agentic-clj" (get-in (first msgs) [:result :serverInfo :name])))
      (is (re-find #"1234\.56" (get-in (second msgs) [:result :content 0 :text]))))))
