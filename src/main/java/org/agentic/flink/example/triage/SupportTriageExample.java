package org.agentic.flink.example.triage;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.inference.ClassifierGuardrail;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.InferenceToolAdapter;
import org.agentic.flink.inference.Scorer;
import org.agentic.flink.inference.djl.DjlInferenceConnection;
import org.agentic.flink.listener.LoggingAgentEventListener;
import org.agentic.flink.listener.MetricsAgentEventListener;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.langchain4j.LangChain4jChatClient;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import dev.langchain4j.model.chat.ChatModel;
import java.util.List;
import java.util.Set;

/**
 * Customer-support triage agent.
 *
 * <p>Pipeline (per inbound ticket):
 *
 * <ol>
 *   <li><b>Guardrail</b> — a sentiment classifier blocks abusive content from reaching the LLM.
 *   <li><b>Intent tool</b> — a topic classifier exposed as a {@code ToolExecutor} categorizes
 *       the ticket (billing / technical / refund / general).
 *   <li><b>Draft</b> — the LLM drafts three candidate replies through the framework's
 *       {@link ChatClient} SPI.
 *   <li><b>Rerank</b> — a cross-encoder {@link Scorer} ranks the candidates against the ticket;
 *       the highest-scoring draft wins.
 *   <li><b>Tone rewrite</b> — for the winning draft, we downcast to
 *       {@link LangChain4jChatClient} and call the underlying
 *       {@link ChatModel} directly. This shows the documented escape hatch in a real
 *       situation: we want LangChain4J's two-arg {@code generate(systemPrompt, userPrompt)}
 *       convenience that the vendor-neutral SPI does not expose.
 * </ol>
 *
 * <p><b>Prerequisites:</b>
 * <pre>
 *   docker compose up -d ollama
 *   docker compose exec ollama ollama pull qwen2.5:3b
 *
 *   # Add to your local pom (or set CLASSPATH):
 *   ai.djl.pytorch:pytorch-native-cpu:0.30.0
 * </pre>
 *
 * <p><b>To run:</b>
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.triage.SupportTriageExample"
 * </pre>
 *
 * <p>See {@code docs/examples/support-triage.md} for the walkthrough.
 */
public class SupportTriageExample {

  /** One inbound support ticket. */
  public record Ticket(String id, String customer, String subject, String body) {
    @Override
    public String toString() {
      return "Ticket[" + id + " from " + customer + "]: " + subject;
    }
  }

  /** A draft reply with its rerank score. */
  public record Draft(String text, double score) {}

  public static void main(String[] args) throws Exception {
    String ollamaUrl = ConfigKeys.DEFAULT_OLLAMA_BASE_URL;
    String llmModel = "qwen2.5:3b";

    // ── 1. Chat backend (Ollama via LangChain4J) ────────────────────────────────────────
    LangChain4jChatConnection chatConn = LangChain4jChatConnection.ollama(ollamaUrl);
    ChatSetup chatSetup =
        ChatSetup.builder()
            .withModel(llmModel)
            .withTemperature(0.6)
            .withMaxResponseTokens(512)
            .build();
    ChatSetup creative =
        chatSetup.toBuilder().withTemperature(0.85).withMaxResponseTokens(256).build();

    // ── 2. Sentiment guardrail (HuggingFace SST-2) ──────────────────────────────────────
    DjlInferenceConnection sentiment =
        DjlInferenceConnection.classification(
            "djl://ai.djl.huggingface.pytorch/distilbert-base-uncased-finetuned-sst-2-english");
    InferenceSetup sentimentSetup =
        InferenceSetup.builder()
            .withModelName("sst-2")
            .withModelUri(sentiment.getDefaultModelUri())
            .build();
    ClassifierGuardrail abuseFilter =
        new ClassifierGuardrail(
            "abuse-filter",
            sentiment,
            sentimentSetup,
            // Treat strongly-negative tickets as needing a human; block the LLM path.
            Set.of("NEGATIVE"),
            true,
            false);

    // ── 3. Intent classifier exposed as a tool ──────────────────────────────────────────
    DjlInferenceConnection intent =
        DjlInferenceConnection.classification(
            "djl://ai.djl.huggingface.pytorch/facebook/bart-large-mnli");
    InferenceToolAdapter intentTool =
        new InferenceToolAdapter(
            "ticket-intent",
            "Classify a support ticket into one of: billing, technical, refund, general.",
            intent,
            InferenceSetup.builder()
                .withModelName("bart-mnli")
                .withModelUri(intent.getDefaultModelUri())
                .build(),
            InferenceToolAdapter.TaskKind.CLASSIFIER);

    // ── 4. Cross-encoder reranker for candidate replies ─────────────────────────────────
    DjlInferenceConnection reranker =
        DjlInferenceConnection.classification(
            "djl://ai.djl.huggingface.pytorch/cross-encoder/ms-marco-MiniLM-L-6-v2");
    InferenceSetup rerankerSetup =
        InferenceSetup.builder()
            .withModelName("ms-marco-MiniLM-L-6-v2")
            .withModelUri(reranker.getDefaultModelUri())
            .build();
    Scorer rerank = reranker.bind(null).asScorer();

    // ── 5. Listeners ────────────────────────────────────────────────────────────────────
    MetricsAgentEventListener metrics = new MetricsAgentEventListener();
    LoggingAgentEventListener logger = new LoggingAgentEventListener();

    // ── 6. Build the agent ──────────────────────────────────────────────────────────────
    Agent agent =
        Agent.builder()
            .withId("support-triage")
            .withName("Support Triage Agent")
            .withSystemPrompt(
                "You are a support agent. Draft a concise, polite reply to the ticket below. "
                    + "Use the customer's name and address the issue specifically.")
            .withChatConnection(chatConn)
            .withChatSetup(chatSetup)
            .withInferenceTool(intentTool)
            .withInferenceConnection("reranker", reranker)
            .withGuardrail(abuseFilter)
            .withListener(logger, metrics)
            .withMaxIterations(2)
            .build();

    // ── 7. Process one sample ticket end-to-end ────────────────────────────────────────
    Ticket ticket =
        new Ticket(
            "T-1023",
            "Alex Morgan",
            "Where is my refund?",
            "I cancelled my order three weeks ago and still see no refund. "
                + "Please investigate ASAP.");

    System.out.println("Triaging " + ticket);

    // Bind the chat client once for this single-shot demo.
    ChatClient chatClient = chatConn.bind(null);

    //  Guardrail: pre-LLM sentiment check.
    var guardDecision = abuseFilter.beforeChat(agent.getAgentId(), List.of(ChatMessage.user(ticket.body())));
    if (guardDecision.isBlock()) {
      System.out.println("Routed to human queue: " + guardDecision.getReason());
      return;
    }

    //  Intent tool call.
    @SuppressWarnings("unchecked")
    java.util.Map<String, Object> intentResult =
        (java.util.Map<String, Object>)
            intentTool.execute(java.util.Map.of("text", ticket.body())).get();
    String intentLabel = (String) intentResult.get("label");
    System.out.println("Intent: " + intentLabel + " (score=" + intentResult.get("score") + ")");

    //  Draft three candidates.
    java.util.List<Draft> drafts = new java.util.ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ChatResponse resp =
          chatClient.chat(
              List.of(
                  ChatMessage.system(agent.getSystemPrompt()
                      + "\nDetected intent: " + intentLabel
                      + "\nThis is draft attempt #" + (i + 1) + "; vary phrasing."),
                  ChatMessage.user("Customer: " + ticket.customer() + "\nTicket: " + ticket.body())),
              creative);
      double score = rerank.scorePair(resp.getText(), ticket.body(), rerankerSetup);
      drafts.add(new Draft(resp.getText(), score));
    }
    drafts.sort((a, b) -> Double.compare(b.score(), a.score()));
    Draft winner = drafts.get(0);
    System.out.println("Picked draft (score=" + winner.score() + ")");

    //  Tone rewrite — uses the LangChain4J escape hatch.
    String polished = tonePass(chatClient, winner.text());
    System.out.println("\n=== Final reply ===\n" + polished);

    System.out.println(
        "\nMetrics — chatRequests="
            + metrics.getChatRequests()
            + " toolCalls="
            + metrics.getToolCalls()
            + " inferences="
            + metrics.getInferences()
            + " guardrailBlocks="
            + metrics.getGuardrailBlocks());
  }

  /**
   * Tone-rewrite pass using LangChain4J directly through the documented escape hatch.
   *
   * <p>The framework's {@link ChatClient} doesn't expose LangChain4J's two-arg {@code
   * generate(system, user)} convenience. Rather than build a new SPI method for it, we
   * downcast — this is what {@link LangChain4jChatClient#getUnderlyingModel()} is for.
   */
  private static String tonePass(ChatClient chatClient, String draft) {
    if (!(chatClient instanceof LangChain4jChatClient lc)) {
      // Not LangChain4J — fall back to a regular chat call.
      ChatResponse r =
          chatClient.chat(
              List.of(
                  ChatMessage.system("Rewrite the reply in a warm, professional tone."),
                  ChatMessage.user(draft)),
              ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.4).build());
      return r.getText();
    }
    // Lazily build the underlying model before reading it (LC4j caches inside the client).
    lc.chat(
        List.of(ChatMessage.user("warmup")),
        ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.0).build());
    ChatModel raw = lc.getUnderlyingModel();
    return raw.chat(
        "Rewrite the reply in a warm, professional tone, keeping the facts intact.\n\n" + draft);
  }
}
