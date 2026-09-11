package org.agentic.flink.inference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** Regression: nested objects/arrays inside tool-call arguments must stay structured. */
class ToolArgumentsTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** Random JSON value tree of bounded depth; leaves are strings, ints, doubles, booleans, null. */
  private static Object randomValue(int depth) {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    int pick = depth <= 0 ? r.nextInt(5) : r.nextInt(7);
    switch (pick) {
      case 0:
        return "s-" + UUID.randomUUID();
      case 1:
        return r.nextInt(-1_000_000, 1_000_000);
      case 2:
        return r.nextInt(1, 1000) / 8.0;
      case 3:
        return r.nextBoolean();
      case 4:
        return null;
      case 5:
        return randomObject(depth - 1);
      default:
        List<Object> list = new ArrayList<>();
        int n = 1 + r.nextInt(4);
        for (int i = 0; i < n; i++) {
          list.add(randomValue(depth - 1));
        }
        return list;
    }
  }

  private static Map<String, Object> randomObject(int depth) {
    Map<String, Object> m = new LinkedHashMap<>();
    int n = 1 + ThreadLocalRandom.current().nextInt(4);
    for (int i = 0; i < n; i++) {
      m.put("k" + i + "-" + UUID.randomUUID().toString().substring(0, 6), randomValue(depth));
    }
    return m;
  }

  @RepeatedTest(10)
  @DisplayName("random nested argument trees round-trip structurally intact")
  void randomTreesRoundTrip() throws Exception {
    Map<String, Object> original = randomObject(3);
    // ensure at least one nested object and one nested array are present
    original.put("nested", randomObject(2));
    original.put("items", List.of(randomObject(1), List.of(1, 2, List.of("x", "y")), "leaf"));
    String json = JSON.writeValueAsString(original);

    Map<String, Object> parsed = ToolArguments.parse(json);
    assertEquals(original, parsed);
    assertInstanceOf(Map.class, parsed.get("nested"));
    assertInstanceOf(List.class, parsed.get("items"));
    assertInstanceOf(Map.class, ((List<?>) parsed.get("items")).get(0));
    assertInstanceOf(List.class, ((List<?>) ((List<?>) parsed.get("items")).get(1)).get(2));

    assertEquals(original, ToolArguments.parse(ToolArguments.toJson(parsed)));
    assertEquals(original, ToolArguments.coerce(json));
    assertEquals(original, ToolArguments.coerce(parsed));
  }

  @Test
  @DisplayName("literal nested payload with escaped strings, brackets and commas inside strings")
  void trickyLiterals() {
    String id = UUID.randomUUID().toString();
    String json =
        "{\"query\":{\"filter\":{\"ids\":[\"" + id + "\",\"a,b\",\"c}d\"],\"limit\":5},"
            + "\"text\":\"say \\\"hi\\\", [not an array]\"},\"tags\":[[1,2],[3,[4,5]]],\"flag\":true}";
    Map<String, Object> args = ToolArguments.parse(json);

    Map<?, ?> query = assertInstanceOf(Map.class, args.get("query"));
    Map<?, ?> filter = assertInstanceOf(Map.class, query.get("filter"));
    assertEquals(List.of(id, "a,b", "c}d"), filter.get("ids"));
    assertEquals(5, filter.get("limit"));
    assertEquals("say \"hi\", [not an array]", query.get("text"));
    assertEquals(List.of(List.of(1, 2), List.of(3, List.of(4, 5))), args.get("tags"));
    assertEquals(Boolean.TRUE, args.get("flag"));
  }

  @Test
  @DisplayName("empty, null and non-object payloads")
  void edgeCases() {
    assertTrue(ToolArguments.parse(null).isEmpty());
    assertTrue(ToolArguments.parse("  ").isEmpty());
    assertTrue(ToolArguments.parse("{}").isEmpty());
    assertThrows(IllegalArgumentException.class, () -> ToolArguments.parse("[1,2]"));
    assertThrows(IllegalArgumentException.class, () -> ToolArguments.parse("{not json"));
    assertThrows(IllegalArgumentException.class, () -> ToolArguments.coerce(42));
  }

  @Test
  @DisplayName("InferenceToolAdapter accepts the raw JSON payload of a tool call")
  void adapterFromJson() throws Exception {
    String label = "label-" + UUID.randomUUID();
    double score = ThreadLocalRandom.current().nextDouble();
    InferenceToolAdapter adapter =
        new InferenceToolAdapter(
            "tool-" + UUID.randomUUID(),
            null,
            new EchoInferenceConnection(label, score, Map.of(label, score), score, 4),
            InferenceSetup.builder().withModelName("m").withModelUri("u").build(),
            InferenceToolAdapter.TaskKind.CLASSIFIER);
    String text = "text-" + UUID.randomUUID();
    String json = "{\"text\":\"" + text + "\",\"options\":{\"topK\":[1,2,3]}}";
    Map<?, ?> result = assertInstanceOf(Map.class, adapter.execute(json).get());
    assertEquals(label, result.get("label"));
  }
}
