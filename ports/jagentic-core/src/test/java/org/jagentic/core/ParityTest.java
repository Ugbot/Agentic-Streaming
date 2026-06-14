package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Cross-core parity guard (Java side). The Python {@code test_parity.py} and Go
 * {@code parity_test.go} assert these exact same golden values, so any core diverging
 * in hashing, embedding, retrieval ranking, or routing is caught.
 */
class ParityTest {

  // Golden FNV-1a 32-bit hashes (unsigned) — identical in all three cores.
  private static final Map<String, Long> GOLDEN_FNV =
      Map.of("crypto", 1712156752L, "balance", 2560266987L, "card", 2284280159L, "dispute", 3025431163L);
  private static final Map<String, Long> GOLDEN_BUCKET_256 =
      Map.of("crypto", 80L, "balance", 235L, "card", 95L, "dispute", 123L);

  @Test
  void fnv1aGolden() {
    for (Map.Entry<String, Long> e : GOLDEN_FNV.entrySet()) {
      long unsigned = Integer.toUnsignedLong(Retrieval.fnv1a32(e.getKey()));
      assertEquals(e.getValue().longValue(), unsigned, e.getKey());
      assertEquals(GOLDEN_BUCKET_256.get(e.getKey()).longValue(), unsigned % 256, e.getKey());
    }
  }

  @Test
  void embedGoldenVector() {
    float[] v = Retrieval.embed("crypto cash", 8);
    List<Integer> nonzero = new ArrayList<>();
    for (int i = 0; i < v.length; i++) {
      if (v[i] != 0.0f) nonzero.add(i);
    }
    assertEquals(List.of(0, 2), nonzero);
    for (int i : nonzero) {
      assertTrue(Math.abs(v[i] - 1.0 / Math.sqrt(2)) < 1e-6, "v[" + i + "]=" + v[i]);
    }
  }

  @Test
  void retrievalRanksCryptoFirst() {
    Retrieval.TwoTierRetriever retr = Banking.retriever();
    List<Retrieval.Scored> hits =
        retr.retrieve(Retrieval.embed("tell me about crypto cash-back redemption", Banking.DIM), 4);
    assertTrue(!hits.isEmpty() && hits.get(0).id().equals("kb_cards_crypto"),
        "top hit should be kb_cards_crypto");
    assertEquals(Banking.KB.get("kb_cards_crypto"), hits.get(0).text());
  }

  @Test
  void routingParity() {
    LocalRuntime rt = new LocalRuntime(Banking.buildGraph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), Banking.defaultTools(), Banking.retriever());
    String[][] cases = {
        {"what card types do you offer?", "cards"},
        {"what is my balance?", "payments"},
        {"how do I dispute a charge?", "payments"},
        {"hello there", "general"},
        {"tell me about crypto cash-back", "cards"},
    };
    for (String[] c : cases) {
      TurnResult res = rt.submit(new Event("c-" + c[0].substring(0, 4), "demo", c[0]));
      assertEquals(c[1], res.path, c[0]);
    }
  }
}
