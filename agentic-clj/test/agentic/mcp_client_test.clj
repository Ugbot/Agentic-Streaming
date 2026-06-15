(ns agentic.mcp-client-test
  (:require [clojure.test :refer [deftest is testing]]
            [agentic.mcp-client :as mcpc]
            [agentic.mcp :as mcp]
            [agentic.tools :as tools]
            [agentic.banking :as banking])
  (:import [java.io PipedInputStream PipedOutputStream InputStreamReader OutputStreamWriter
            BufferedReader BufferedWriter]
           [java.nio.charset StandardCharsets]))

(deftest mcp-client-talks-to-real-server-in-process
  ;; Wire the client to the EXISTING agentic.mcp server over two piped-stream channels:
  ;;   client-out -> server-in   (client requests)
  ;;   server-out -> client-in   (server responses)
  (let [server-reg (banking/default-tools)
        ;; channel 1: client writes, server reads
        c2s-out (PipedOutputStream.)
        c2s-in  (PipedInputStream. c2s-out)
        ;; channel 2: server writes, client reads
        s2c-out (PipedOutputStream.)
        s2c-in  (PipedInputStream. s2c-out)
        client-writer (BufferedWriter. (OutputStreamWriter. c2s-out StandardCharsets/UTF_8))
        client-reader (BufferedReader. (InputStreamReader. s2c-in StandardCharsets/UTF_8))
        server-thread (Thread. #(mcp/serve server-reg c2s-in s2c-out))]
    (.setDaemon server-thread true)
    (.start server-thread)
    (try
      (let [client (mcpc/mcp-client-from-streams client-reader client-writer)]
        (testing "list-tools surfaces the server's registered tools"
          (is (contains? (set (map :name (mcpc/list-tools client))) "get_balance")))
        (testing "call-tool returns the joined text content"
          (let [out (mcpc/call-tool client "get_balance" {})]
            (is (string? out))
            (is (re-find #"1234\.56" out))))
        (testing "register exposes MCP tools in a registry under the prefix"
          (let [reg (tools/registry)]
            (mcpc/register client reg "mcp_")
            (is (contains? (set (tools/ids reg)) "mcp_get_balance"))
            (is (re-find #"1234\.56" (str (tools/execute reg "mcp_get_balance" {})))))))
      (finally
        (.close client-writer)
        (.interrupt server-thread)))))
