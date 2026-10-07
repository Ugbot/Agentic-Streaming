package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Timestamp;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The {@link PostgresChangeChannel.Cursor} split position encoding. */
final class PostgresChangeChannelCursorTest {

  @Test
  @DisplayName("encode/decode round-trips timestamp nanos and ids containing delimiter characters")
  void roundTrip() {
    Timestamp ts = new Timestamp(ThreadLocalRandom.current().nextLong(0, 4_000_000_000_000L));
    ts.setNanos(ThreadLocalRandom.current().nextInt(0, 1_000_000_000));
    String flowId = "flow:" + UUID.randomUUID() + " %/\u00e9";
    String factId = "fact:" + UUID.randomUUID() + ":x";
    PostgresChangeChannel.Cursor cursor = new PostgresChangeChannel.Cursor(ts, flowId, factId);

    PostgresChangeChannel.Cursor back = PostgresChangeChannel.Cursor.decode(cursor.encode());

    assertEquals(cursor, back);
    assertEquals(ts, back.createdAt);
    assertEquals(ts.getNanos(), back.createdAt.getNanos());
    assertEquals(flowId, back.flowId);
    assertEquals(factId, back.factId);
    assertNotEquals(cursor, PostgresChangeChannel.Cursor.START);
  }

  @Test
  @DisplayName("a null position decodes to the start cursor, which sorts before every row")
  void nullIsStart() {
    assertSame(PostgresChangeChannel.Cursor.START, PostgresChangeChannel.Cursor.decode(null));
    assertEquals(0L, PostgresChangeChannel.Cursor.START.createdAt.getTime());
    assertEquals("", PostgresChangeChannel.Cursor.START.flowId);
    assertEquals("", PostgresChangeChannel.Cursor.START.factId);
  }

  @Test
  @DisplayName(
      "malformed positions are rejected rather than silently restarting from the beginning")
  void malformedRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresChangeChannel.Cursor.decode("not-a-cursor-" + UUID.randomUUID()));
    assertThrows(NumberFormatException.class, () -> PostgresChangeChannel.Cursor.decode("x:0:a:b"));
  }
}
