package org.agentic.flink.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.memory.InMemoryLongTermStore;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MemorySetAccessorTest {

  private InMemoryLongTermStore store;
  private MemorySetAccessor accessor;
  private String flowId;

  @BeforeEach
  void setUp() throws Exception {
    store = new InMemoryLongTermStore();
    store.initialize(new HashMap<>());
    accessor = new MemorySetAccessor(store);
    flowId = "flow-" + UUID.randomUUID();
  }

  @Test
  @DisplayName("save() then load() of a named set is lossless")
  void saveAndLoadRoundTrip() throws Exception {
    MemorySet facts = new MemorySet("facts");
    int n = ThreadLocalRandom.current().nextInt(3, 12);
    for (int i = 0; i < n; i++) {
      ContextItem item =
          new ContextItem(
              "fact-" + UUID.randomUUID(), ContextPriority.MUST, MemoryType.LONG_TERM);
      facts.add(item);
    }

    accessor.save(flowId, facts);
    MemorySet loaded = accessor.load(flowId, "facts");

    assertEquals(facts.size(), loaded.size());
    for (String id : facts.getItems().keySet()) {
      assertNotNull(loaded.get(id));
      assertEquals(facts.get(id).getContent(), loaded.get(id).getContent());
    }
  }

  @Test
  @DisplayName("Two distinct named sets share the fact bucket without colliding")
  void namespaceIsolation() throws Exception {
    MemorySet facts = new MemorySet("facts");
    facts.add(new ContextItem("user prefers SI units", ContextPriority.MUST, MemoryType.LONG_TERM));

    MemorySet decisions = new MemorySet("decisions");
    decisions.add(
        new ContextItem("validated order 42", ContextPriority.MUST, MemoryType.LONG_TERM));

    accessor.save(flowId, facts);
    accessor.save(flowId, decisions);

    MemorySet loadedFacts = accessor.load(flowId, "facts");
    MemorySet loadedDecisions = accessor.load(flowId, "decisions");

    assertEquals(1, loadedFacts.size());
    assertEquals(1, loadedDecisions.size());
    assertTrue(
        loadedFacts.entries().iterator().next().getContent().contains("SI units"));
    assertTrue(
        loadedDecisions.entries().iterator().next().getContent().contains("order 42"));
  }

  @Test
  @DisplayName("addItem() and removeItem() work without rewriting the whole slice")
  void incrementalUpdates() throws Exception {
    ContextItem item =
        new ContextItem(
            "fact-" + UUID.randomUUID(), ContextPriority.SHOULD, MemoryType.LONG_TERM);

    accessor.addItem(flowId, "facts", item);
    MemorySet loaded = accessor.load(flowId, "facts");
    assertEquals(1, loaded.size());
    assertNotNull(loaded.get(item.getItemId()));

    accessor.removeItem(flowId, "facts", item.getItemId());
    MemorySet afterRemoval = accessor.load(flowId, "facts");
    assertEquals(0, afterRemoval.size());
    assertFalse(afterRemoval.getItems().containsKey(item.getItemId()));
  }
}
