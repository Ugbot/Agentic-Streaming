package org.agentic.flink.memory.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScoredItemTest {

  @Test
  @DisplayName("Natural order is descending by score so top-k comes first")
  void sortsDescendingByScore() {
    List<ScoredItem> items = new ArrayList<>();
    int n = ThreadLocalRandom.current().nextInt(10, 50);
    for (int i = 0; i < n; i++) {
      double score = ThreadLocalRandom.current().nextDouble(-1.0, 1.0);
      items.add(new ScoredItem(UUID.randomUUID().toString(), score, null));
    }

    Collections.sort(items);

    for (int i = 1; i < items.size(); i++) {
      assertTrue(
          items.get(i - 1).getScore() >= items.get(i).getScore(),
          "Expected descending order at index " + i);
    }
  }

  @Test
  @DisplayName("Ties between equal scores are allowed and stable enough for comparators")
  void handlesTies() {
    List<ScoredItem> items = new ArrayList<>();
    items.add(new ScoredItem("a", 0.5, null));
    items.add(new ScoredItem("b", 0.5, null));
    items.add(new ScoredItem("c", 0.9, null));

    Collections.sort(items);

    assertEquals("c", items.get(0).getId());
    // The two tied entries occupy positions 1 and 2 in either order.
    assertTrue(items.get(1).getScore() == 0.5);
    assertTrue(items.get(2).getScore() == 0.5);
  }
}
