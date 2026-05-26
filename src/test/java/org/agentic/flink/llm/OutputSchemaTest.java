package org.agentic.flink.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ThreadLocalRandom;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OutputSchemaTest {

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Verdict {
    private boolean accepted;
    private double confidence;
    private String reason;
  }

  @Test
  @DisplayName("OutputSchema.of() infers a non-empty JSON schema with all declared fields")
  void inferredSchemaListsFields() {
    OutputSchema<Verdict> schema = OutputSchema.of(Verdict.class);
    String json = schema.getJsonSchema();
    assertNotNull(json);
    assertTrue(json.contains("\"accepted\""));
    assertTrue(json.contains("\"confidence\""));
    assertTrue(json.contains("\"reason\""));
    assertTrue(json.contains("\"type\":\"boolean\""));
    assertTrue(json.contains("\"type\":\"number\""));
    assertTrue(json.contains("\"type\":\"string\""));
  }

  @Test
  @DisplayName("Parses a raw JSON LLM response")
  void parsesRawJson() throws Exception {
    OutputSchema<Verdict> schema = OutputSchema.of(Verdict.class);
    double confidence = ThreadLocalRandom.current().nextDouble();
    String reason = "reason-" + ThreadLocalRandom.current().nextInt();
    String raw =
        "{\"accepted\":true,\"confidence\":" + confidence + ",\"reason\":\"" + reason + "\"}";
    Verdict v = schema.parse(raw);
    assertTrue(v.isAccepted());
    assertEquals(confidence, v.getConfidence(), 1e-9);
    assertEquals(reason, v.getReason());
  }

  @Test
  @DisplayName("Strips markdown fences before parsing")
  void stripsMarkdownFences() throws Exception {
    OutputSchema<Verdict> schema = OutputSchema.of(Verdict.class);
    String wrapped =
        "Sure, here is my verdict:\n\n"
            + "```json\n"
            + "{\"accepted\":false,\"confidence\":0.42,\"reason\":\"missing-evidence\"}\n"
            + "```\n";
    Verdict v = schema.parse(wrapped);
    assertEquals(false, v.isAccepted());
    assertEquals(0.42, v.getConfidence(), 1e-9);
    assertEquals("missing-evidence", v.getReason());
  }

  @Test
  @DisplayName("Throws SchemaViolation on garbage input")
  void rejectsGarbage() {
    OutputSchema<Verdict> schema = OutputSchema.of(Verdict.class);
    assertThrows(OutputSchema.SchemaViolation.class, () -> schema.parse("not even close"));
  }
}
