package org.agentic.flink.channel.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The {@link FactBatch} committable serializer. */
final class FactBatchSerializerTest {

  @Test
  @DisplayName("round-trips the commit id and every fact, including non-ASCII JSON")
  void roundTrip() throws Exception {
    List<FactBatch.Fact> facts = new ArrayList<>();
    int n = 1 + ThreadLocalRandom.current().nextInt(20);
    for (int i = 0; i < n; i++) {
      facts.add(
          new FactBatch.Fact(
              "flow-" + UUID.randomUUID(),
              "fact-" + i,
              "{\"content\":\"caf\u00e9 " + UUID.randomUUID() + "\"}",
              ThreadLocalRandom.current().nextLong(0, 4_000_000_000_000L)));
    }
    FactBatch batch = new FactBatch(UUID.randomUUID().toString(), facts);

    assertEquals(1, FactBatch.SERIALIZER.getVersion());
    byte[] bytes = FactBatch.SERIALIZER.serialize(batch);
    FactBatch back = FactBatch.SERIALIZER.deserialize(1, bytes);
    assertEquals(batch, back);
    assertEquals(batch.commitId(), back.commitId());
    assertEquals(facts, back.facts());
  }

  @Test
  @DisplayName("an empty batch and an unknown version are handled explicitly")
  void emptyAndUnknownVersion() throws Exception {
    FactBatch empty = new FactBatch(UUID.randomUUID().toString(), List.of());
    assertEquals(empty, FactBatch.SERIALIZER.deserialize(1, FactBatch.SERIALIZER.serialize(empty)));
    assertThrows(IOException.class, () -> FactBatch.SERIALIZER.deserialize(2, new byte[0]));
  }
}
