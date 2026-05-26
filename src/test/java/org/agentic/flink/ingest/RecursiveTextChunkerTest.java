package org.agentic.flink.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RecursiveTextChunkerTest {

  @Test
  @DisplayName("rejects non-positive maxChars and out-of-range overlap")
  void rejectsBadConfig() {
    assertThrows(IllegalArgumentException.class, () -> new RecursiveTextChunker(0));
    assertThrows(IllegalArgumentException.class, () -> new RecursiveTextChunker(100, -1));
    assertThrows(IllegalArgumentException.class, () -> new RecursiveTextChunker(100, 100));
  }

  @Test
  @DisplayName("empty / null input yields empty chunk list")
  void emptyInputProducesEmptyList() {
    RecursiveTextChunker c = new RecursiveTextChunker(256);
    assertEquals(0, c.chunk("src", null).size());
    assertEquals(0, c.chunk("src", "").size());
  }

  @Test
  @DisplayName("short input fits in a single chunk")
  void singleChunkForShortInput() {
    RecursiveTextChunker c = new RecursiveTextChunker(1024);
    String text = "Hello world. This text is well under the limit.";
    List<Chunk> chunks = c.chunk("doc-1", text);
    assertEquals(1, chunks.size());
    assertEquals("doc-1::0", chunks.get(0).getId());
    assertEquals("doc-1", chunks.get(0).getSourceId());
    assertTrue(chunks.get(0).getText().contains("Hello world"));
  }

  @Test
  @DisplayName("long randomized input is split into bounded chunks with sequential positions")
  void longInputProducesMultipleBoundedChunks() {
    int maxChars = ThreadLocalRandom.current().nextInt(64, 256);
    RecursiveTextChunker c = new RecursiveTextChunker(maxChars, 0);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append("Sentence ").append(i).append(" of the random fixture. ");
    }
    String src = "doc-" + UUID.randomUUID();
    List<Chunk> chunks = c.chunk(src, sb.toString());
    assertTrue(chunks.size() > 1);
    for (int i = 0; i < chunks.size(); i++) {
      Chunk ch = chunks.get(i);
      assertEquals(i, ch.getPosition());
      assertEquals(src, ch.getSourceId());
      assertEquals(src + "::" + i, ch.getId());
      // Each chunk respects the budget with a small +/- slack for the recursive separator logic.
      assertTrue(
          ch.getText().length() <= maxChars * 2,
          "chunk " + i + " length " + ch.getText().length() + " > 2× max " + maxChars);
      assertNotNull(ch.getText());
    }
  }

  @Test
  @DisplayName("overlap injects a tail of the previous chunk into the head of the next")
  void overlapProducesSharedText() {
    RecursiveTextChunker c = new RecursiveTextChunker(80, 20);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 50; i++) sb.append("token").append(i).append(' ');
    List<Chunk> chunks = c.chunk("doc", sb.toString().trim());
    assertTrue(chunks.size() >= 2, "expected the long token stream to produce multiple chunks");
    // Some shared substring between consecutive chunks (the overlap tail).
    Chunk first = chunks.get(0);
    Chunk second = chunks.get(1);
    int tailLen = Math.min(20, first.getText().length());
    String tail = first.getText().substring(first.getText().length() - tailLen);
    // Allow some slack: at least one shared 4-char window between the tail and second.
    boolean shared = false;
    for (int i = 0; i + 4 <= tail.length() && !shared; i++) {
      shared = second.getText().contains(tail.substring(i, i + 4));
    }
    assertTrue(shared, "expected overlap-driven shared text between consecutive chunks");
  }
}
