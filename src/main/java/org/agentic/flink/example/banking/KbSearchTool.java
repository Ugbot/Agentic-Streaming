package org.agentic.flink.example.banking;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import org.agentic.flink.tools.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keyword (BM25-ish) knowledge-base search for the CS agent, over the public {@code kb/documents}
 * JSON files ({@code {id, title, content}}). A {@link ToolExecutor} the agent calls as
 * {@code kb_search(query, top_k)}.
 *
 * <p>Deliberately dependency-free (no Redis/embeddings) so the demo runs with only an LLM key — the
 * "swap" to the template's Redis + gemini-embedding vector index is a drop-in replacement behind
 * this same tool name. Documents are loaded once at construction.
 */
public final class KbSearchTool implements ToolExecutor {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(KbSearchTool.class);
  private static final Pattern WORD = Pattern.compile("\\w+");
  private static final int MAX_CONTENT = 1500;

  private final List<Doc> docs;

  private KbSearchTool(List<Doc> docs) {
    this.docs = docs;
  }

  /** Load every {@code *.json} under {@code kbDir}. */
  public static KbSearchTool fromDirectory(String kbDir) {
    List<Doc> docs = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    File dir = new File(kbDir);
    File[] files = dir.listFiles((d, n) -> n.endsWith(".json"));
    if (files != null) {
      for (File f : files) {
        try {
          Map<?, ?> m = mapper.readValue(Files.readAllBytes(Path.of(f.getPath())), Map.class);
          docs.add(
              new Doc(
                  str(m.get("id")), str(m.get("title")), str(m.get("content"))));
        } catch (IOException e) {
          LOG.warn("Skipping unreadable KB doc {}: {}", f.getName(), e.getMessage());
        }
      }
    }
    LOG.info("Loaded {} KB documents from {}", docs.size(), kbDir);
    return new KbSearchTool(docs);
  }

  @Override
  public String getToolId() {
    return "kb_search";
  }

  @Override
  public String getDescription() {
    return "Search the Rho-Bank knowledge base. Args: query (string), top_k (int, default 4)."
        + " Returns matching documents with title and content.";
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    String query = parameters == null ? "" : String.valueOf(parameters.getOrDefault("query", ""));
    int topK = 4;
    Object k = parameters == null ? null : parameters.get("top_k");
    if (k instanceof Number) {
      topK = ((Number) k).intValue();
    }
    return CompletableFuture.completedFuture(search(query, Math.max(1, Math.min(topK, 10))));
  }

  private List<Map<String, Object>> search(String query, int topK) {
    List<String> terms = new ArrayList<>();
    var m = WORD.matcher(query.toLowerCase(Locale.ROOT));
    while (m.find()) {
      terms.add(m.group());
    }
    List<Doc> ranked = new ArrayList<>(docs);
    ranked.sort((a, b) -> Integer.compare(score(b, terms), score(a, terms)));
    List<Map<String, Object>> out = new ArrayList<>();
    for (int i = 0; i < Math.min(topK, ranked.size()); i++) {
      Doc d = ranked.get(i);
      if (score(d, terms) == 0) {
        break;
      }
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("doc_id", d.id);
      row.put("title", d.title);
      row.put("content", d.content.length() > MAX_CONTENT ? d.content.substring(0, MAX_CONTENT) : d.content);
      out.add(row);
    }
    return out;
  }

  private static String str(Object o) {
    return o == null ? "" : o.toString();
  }

  private static int score(Doc d, List<String> terms) {
    if (terms.isEmpty()) {
      return 0;
    }
    String hay = (d.title + " " + d.content).toLowerCase(Locale.ROOT);
    int s = 0;
    for (String t : terms) {
      int idx = 0;
      while ((idx = hay.indexOf(t, idx)) >= 0) {
        s++;
        idx += t.length();
        if (s > 1000) {
          break;
        }
      }
    }
    return s;
  }

  // Serializable: the tool is held by a Flink path operator (KNOWLEDGE/DISPUTE brain) and shipped
  // with it, so its loaded docs must serialize into the operator.
  private static final class Doc implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    final String id;
    final String title;
    final String content;

    Doc(String id, String title, String content) {
      this.id = id;
      this.title = title;
      this.content = content;
    }
  }
}
