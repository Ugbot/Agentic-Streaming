package org.agentic.flink.tools.rag;

import org.agentic.flink.rag.KnowledgeBase;
import org.agentic.flink.tools.AbstractToolExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Agent-callable tool: scrape a web page and index it into a shared {@link KnowledgeBase}.
 *
 * <p>Lets an agent (or a supervisor planning loop) push its own scraping targets into the
 * knowledge base — e.g. "fetch this docs page so I can answer questions about it". Parameter:
 * {@code url} (required). Returns the title and number of chunks indexed.
 */
public final class ScrapeUrlTool extends AbstractToolExecutor {

  private final KnowledgeBase knowledgeBase;

  public ScrapeUrlTool(KnowledgeBase knowledgeBase) {
    super("scrape_url", "Scrape a web page and add its content to the knowledge base");
    this.knowledgeBase = knowledgeBase;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    logExecution(parameters);
    return CompletableFuture.supplyAsync(
        () -> {
          String url = getRequiredParameter(parameters, "url", String.class);
          KnowledgeBase.IngestResult r = knowledgeBase.ingestUrl(url);
          Map<String, Object> result = new HashMap<>();
          result.put("url", r.url);
          result.put("title", r.title);
          result.put("chunks_indexed", r.chunks);
          result.put("ok", r.ok);
          if (!r.ok) result.put("error", r.error);
          return result;
        });
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    return super.validateParameters(parameters)
        && parameters.get("url") instanceof String;
  }
}
