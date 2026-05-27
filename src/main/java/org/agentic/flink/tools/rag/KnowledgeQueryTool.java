package org.agentic.flink.tools.rag;

import org.agentic.flink.rag.KnowledgeBase;
import org.agentic.flink.tools.AbstractToolExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Agent-callable tool: answer a question from the shared {@link KnowledgeBase} using retrieval +
 * the configured chat model (Claude by default).
 *
 * <p>Parameters: {@code question} (required), {@code top_k} (optional int, default 4). Returns the
 * grounded answer plus the source passages it cited.
 */
public final class KnowledgeQueryTool extends AbstractToolExecutor {

  private final KnowledgeBase knowledgeBase;

  public KnowledgeQueryTool(KnowledgeBase knowledgeBase) {
    super("ask_knowledge_base", "Answer a question using the scraped knowledge base (RAG)");
    this.knowledgeBase = knowledgeBase;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    logExecution(parameters);
    return CompletableFuture.supplyAsync(
        () -> {
          String question = getRequiredParameter(parameters, "question", String.class);
          int topK = getOptionalParameter(parameters, "top_k", Integer.class, 4);
          KnowledgeBase.Answer answer = knowledgeBase.ask(question, topK);

          List<Map<String, Object>> sources = new ArrayList<>();
          for (KnowledgeBase.Passage p : answer.sources) {
            Map<String, Object> s = new HashMap<>();
            s.put("id", p.id);
            s.put("url", p.url);
            s.put("title", p.title);
            s.put("score", p.score);
            sources.add(s);
          }

          Map<String, Object> result = new HashMap<>();
          result.put("question", question);
          result.put("answer", answer.text);
          result.put("sources", sources);
          return result;
        });
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    return super.validateParameters(parameters)
        && parameters.get("question") instanceof String;
  }
}
