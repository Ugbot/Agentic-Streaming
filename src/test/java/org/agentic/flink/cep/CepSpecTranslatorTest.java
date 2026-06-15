package org.agentic.flink.cep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.cep.pattern.Pattern;
import org.junit.jupiter.api.Test;

/** Phase C: the same declarative {@code cep:} rule translates to a native Flink {@link Pattern}.
 * Verifies the stage chain + the {@code where} conditions without needing a cluster. */
class CepSpecTranslatorTest {

  /** A tiny stream event for the test: text + metadata. */
  private record Ev(String text, Map<String, String> meta) {}

  @SuppressWarnings("unchecked")
  private static Map<String, Object> incidentSpec() {
    return Map.of(
        "name", "incident", "within", 300000,
        "pattern", List.of(
            Map.of("stage", "first", "where", Map.of("text_contains", "anomaly")),
            Map.of("stage", "second", "where", Map.of("text_contains", "anomaly"), "contiguity", "followedBy"),
            Map.of("stage", "third", "where", Map.of("text_contains", "anomaly"), "contiguity", "followedBy")));
  }

  @Test
  void translatesToAFlinkPatternChain() {
    Pattern<Ev, Ev> pattern = CepSpecTranslator.toPattern(incidentSpec(), Ev::text, Ev::meta);

    // Walk the chain tail → head; the names must be the declared stages in order.
    List<String> names = new ArrayList<>();
    for (Pattern<Ev, ?> cur = pattern; cur != null; cur = cur.getPrevious()) {
      names.add(cur.getName());
    }
    java.util.Collections.reverse(names);
    assertEquals(List.of("first", "second", "third"), names);
  }

  @Test
  void conditionsReflectTheWhereMiniLanguage() throws Exception {
    var textContains = CepSpecTranslator.<Ev>condition(Map.of("text_contains", "anomaly"), Ev::text, Ev::meta);
    assertTrue(textContains.filter(new Ev("anomaly cpu", Map.of())));
    assertFalse(textContains.filter(new Ev("all good", Map.of())));

    var gt = CepSpecTranslator.<Ev>condition(Map.of("metadata_gt", Map.of("score", 0.9)), Ev::text, Ev::meta);
    assertTrue(gt.filter(new Ev("x", Map.of("score", "0.95"))));
    assertFalse(gt.filter(new Ev("x", Map.of("score", "0.5"))));

    var any = CepSpecTranslator.<Ev>condition("any", Ev::text, Ev::meta);
    assertTrue(any.filter(new Ev("whatever", Map.of())));
  }
}
