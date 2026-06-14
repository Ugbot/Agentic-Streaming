package org.agentic.flink.example.banking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.embedding.HashEmbeddingConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Plumbing test for {@link VectorKbSearchTool} with deterministic hash embeddings: build → embed →
 * HNSW index → search → cache. (Semantic recall is verified live with DJL/Gemini; hash vectors
 * aren't semantic, so the assertions use exact-text matches, which hash to identical vectors.)
 */
class VectorKbSearchToolTest {

  private static void writeDoc(Path dir, String id, String title, String content) throws Exception {
    String json =
        "{\"id\":\"" + id + "\",\"title\":\"" + title + "\",\"content\":\"" + content + "\"}";
    Files.writeString(dir.resolve(id + ".json"), json);
  }

  @Test
  @DisplayName("embeds docs into HNSW and retrieves the matching doc; respects top_k; caches")
  void buildSearchCache(@TempDir Path kb, @TempDir Path cache) throws Exception {
    writeDoc(kb, "doc_a", "Blue Account", "The Blue checking account has no monthly fee.");
    writeDoc(kb, "doc_b", "Crypto Cash Back Card", "Earns 2 percent cash back on all purchases.");
    writeDoc(kb, "doc_c", "Gold Savings", "The Gold savings account pays tiered interest.");

    HashEmbeddingConnection conn = new HashEmbeddingConnection();
    EmbeddingSetup setup = EmbeddingSetup.of("hash", 256, true);

    VectorKbSearchTool tool =
        VectorKbSearchTool.build(kb.toString(), cache.toString(), conn, setup);
    assertEquals("kb_search", tool.getToolId());

    // Query == doc_b's indexed text ("title\ncontent") → identical hash vector → top-1 is doc_b.
    String exact = "Crypto Cash Back Card\nEarns 2 percent cash back on all purchases.";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> hits =
        (List<Map<String, Object>>) tool.execute(Map.of("query", exact, "top_k", 1)).join();
    assertEquals(1, hits.size());
    assertEquals("doc_b", hits.get(0).get("doc_id"));
    assertEquals("Crypto Cash Back Card", hits.get(0).get("title"));

    // top_k respected.
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> top3 =
        (List<Map<String, Object>>) tool.execute(Map.of("query", exact, "top_k", 3)).join();
    assertTrue(top3.size() <= 3 && !top3.isEmpty());

    // A cache file was written, and a rebuild loads from it (no re-embed needed).
    assertTrue(
        Files.list(cache).anyMatch(p -> p.getFileName().toString().startsWith("kb_vectors_")),
        "expected an embedding cache file");
    VectorKbSearchTool reloaded =
        VectorKbSearchTool.build(kb.toString(), cache.toString(), conn, setup);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> again =
        (List<Map<String, Object>>) reloaded.execute(Map.of("query", exact, "top_k", 1)).join();
    assertEquals("doc_b", again.get(0).get("doc_id"));
  }
}
