package org.jagentic.tools.rag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.jagentic.core.ToolRegistry;
import org.jagentic.tools.inference.InferencePack;

/** RAG pack (chunk→embed→store→search/answer, model-free hashing embedder + in-memory store)
 * and the self-contained inference pack (classify/score/guardrail with the classifier defined
 * in the call). All offline/deterministic. */
class RagInferencePackTest {

  private ToolRegistry ragReg() {
    ToolRegistry reg = new ToolRegistry();
    new RagPack().register(reg);
    return reg;
  }

  private ToolRegistry inferenceReg() {
    ToolRegistry reg = new ToolRegistry();
    new InferencePack().register(reg);
    return reg;
  }

  @Test
  @SuppressWarnings("unchecked")
  void ingestThenSearchRetrievesRelevantChunk() {
    ToolRegistry reg = ragReg();
    Map<String, Object> ing = (Map<String, Object>) reg.execute("ingest_document", Map.of(
        "content", "Platinum cards carry an annual fee. "
            + "To dispute a charge, open the transaction and tap Dispute within sixty days. "
            + "Crypto cash-back can be redeemed to a linked wallet.",
        "chunk_size", 60, "chunk_overlap", 10, "source_id", "kb"));
    assertTrue(((Number) ing.get("chunks_indexed")).intValue() >= 2);

    Map<String, Object> search = (Map<String, Object>) reg.execute("semantic_search",
        Map.of("query", "how do I dispute a charge", "k", 1));
    List<Map<String, Object>> results = (List<Map<String, Object>>) search.get("results");
    assertFalse(results.isEmpty());
    assertTrue(((String) results.get(0).get("text")).toLowerCase().contains("dispute"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void ragAnswerReturnsExtractiveAnswerWithSources() {
    ToolRegistry reg = ragReg();
    reg.execute("ingest_document", Map.of("content",
        "Daily transfer limits are ten thousand by default; raise them in settings.",
        "chunk_size", 80, "source_id", "limits"));
    Map<String, Object> ans = (Map<String, Object>) reg.execute("rag_answer",
        Map.of("query", "what is the transfer limit", "k", 1));
    assertTrue(((String) ans.get("answer")).toLowerCase().contains("transfer limit")
        || ((String) ans.get("answer")).toLowerCase().contains("ten thousand"));
    assertFalse(((List<?>) ans.get("sources")).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void classifyTextWithLexicon() {
    Map<String, Object> r = (Map<String, Object>) inferenceReg().execute("classify_text", Map.of(
        "text", "I want a refund for this charge",
        "lexicon", Map.of(
            "billing", List.of("refund", "charge", "invoice"),
            "tech", List.of("error", "crash", "login")),
        "default_label", "other"));
    assertEquals("billing", r.get("label"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void classifyTextWithExamplesNearestCentroid() {
    Map<String, Object> r = (Map<String, Object>) inferenceReg().execute("classify_text", Map.of(
        "text", "the app crashed when I tried to log in",
        "examples", Map.of(
            "billing", List.of("refund my invoice", "dispute a charge"),
            "tech", List.of("app crashed on login", "error message bug"))));
    assertEquals("tech", r.get("label"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void guardrailCheckBlocksToxicLabel() {
    ToolRegistry reg = inferenceReg();
    Map<String, Object> blocked = (Map<String, Object>) reg.execute("guardrail_check", Map.of(
        "text", "you stupid idiot",
        "blocked", List.of("toxic"),
        "threshold", 0.3,
        "lexicon", Map.of("toxic", List.of("idiot", "stupid", "hate"),
            "ok", List.of("please", "thanks", "help"))));
    assertEquals(true, blocked.get("blocked"));

    Map<String, Object> ok = (Map<String, Object>) reg.execute("guardrail_check", Map.of(
        "text", "please help me, thanks",
        "blocked", List.of("toxic"),
        "threshold", 0.3,
        "lexicon", Map.of("toxic", List.of("idiot", "stupid", "hate"),
            "ok", List.of("please", "thanks", "help"))));
    assertEquals(false, ok.get("blocked"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void scoreTextReturnsLabelProbability() {
    Map<String, Object> r = (Map<String, Object>) inferenceReg().execute("score_text", Map.of(
        "text", "refund my invoice now",
        "label", "billing",
        "lexicon", Map.of("billing", List.of("refund", "invoice"), "tech", List.of("error"))));
    assertTrue(((Number) r.get("score")).doubleValue() > 0.5);
  }
}
