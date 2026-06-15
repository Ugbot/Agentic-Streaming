(ns agentic.llm
  "LLM brains. ChatClient is a protocol returning the shared JSON-mode ReAct result
   ({:tool .. :args ..} | {:text ..}); stub-chat-client is the offline default; ollama/openai use
   clj-http. llm-brain runs the bounded ReAct loop over the conversation transcript, mirroring
   jagentic-core LlmBrain."
  (:require [clojure.data.json :as json]
            [clj-http.client :as http]
            [agentic.tools :as tools]
            [agentic.context :as ctx]
            [agentic.store :as store]))

(defprotocol ChatClient
  (chat [this messages tools] "messages: [{:role :content}], tools: [{:name :description}] -> {:tool :args} | {:text}"))

(def react-system
  (str "You are a tool-using assistant. Reply with a single JSON object: "
       "{\"tool\": <name>, \"args\": {..}} to call a tool, or {\"text\": <answer>} to answer. "
       "Available tools: "))

(defn parse-react
  "Parse a model's JSON reply into {:tool :args} | {:text}."
  [s]
  (try
    (let [m (json/read-str (str s) :key-fn keyword)]
      (cond
        (:tool m) {:tool (:tool m) :args (or (:args m) {})}
        (contains? m :text) {:text (:text m)}
        :else {:text (str s)}))
    (catch Exception _ {:text (str s)})))

;; ---- stub (offline, scripted) ----

(defrecord StubChatClient [!script]
  ChatClient
  (chat [_ _messages _tools]
    (let [script @!script]
      (if (seq script)
        (let [head (first script)]
          (when (next script) (reset! !script (rest script)))
          head)
        {:text "ok"}))))

(defn stub-chat-client [& responses]
  (->StubChatClient (atom (vec responses))))

;; ---- real providers (opt-in; clj-http) ----

(defn ollama-chat-client [{:keys [base-url model] :or {base-url "http://localhost:11434" model "qwen2.5:3b"}}]
  (reify ChatClient
    (chat [_ messages _tools]
      (let [resp (http/post (str base-url "/api/chat")
                            {:body (json/write-str {:model model :messages messages :stream false :format "json"})
                             :content-type :json :as :json :socket-timeout 60000 :connection-timeout 10000})]
        (parse-react (get-in resp [:body :message :content]))))))

(defn openai-chat-client [{:keys [base-url model api-key]
                           :or {base-url "https://api.openai.com/v1" model "gpt-4o-mini"}}]
  (reify ChatClient
    (chat [_ messages _tools]
      (let [resp (http/post (str base-url "/chat/completions")
                            {:headers {"Authorization" (str "Bearer " (or api-key (System/getenv "OPENAI_API_KEY")))}
                             :body (json/write-str {:model model :messages messages
                                                    :response_format {:type "json_object"}})
                             :content-type :json :as :json :socket-timeout 60000})]
        (parse-react (get-in resp [:body :choices 0 :message :content]))))))

;; ---- the ReAct-loop brain ----

(defn llm-brain
  "A brain fn driving a bounded ReAct loop over `chat-client`. opts: :name :system-prompt
   :allowed-tools :max-iterations."
  [chat-client {:keys [name system-prompt allowed-tools max-iterations]
                :or {name "agent" system-prompt "" max-iterations 6}}]
  (fn [_user-text context]
    (let [all-specs (tools/specs (:tools context))
          specs (if allowed-tools
                  (filterv #(contains? (set allowed-tools) (:name %)) all-specs)
                  all-specs)
          sys {:role "system" :content (str system-prompt "\n" react-system (json/write-str specs))}
          transcript (mapv (fn [m] {:role (:role m) :content (:content m)})
                           (store/history (:store context) (:conversation-id context)))]
      (loop [messages (into [sys] transcript) i 0]
        (if (>= i max-iterations)
          (str "[" name "] (stopped after " max-iterations " steps)")
          (let [r (chat chat-client messages specs)]
            (if (:tool r)
              (let [obs (ctx/call-tool context (:tool r) (:args r))]
                (recur (conj messages
                             {:role "assistant" :content (json/write-str {:tool (:tool r) :args (:args r)})}
                             {:role "tool" :content (str obs)})
                       (inc i)))
              (str "[" name "] " (or (:text r) "(no answer)")))))))))
