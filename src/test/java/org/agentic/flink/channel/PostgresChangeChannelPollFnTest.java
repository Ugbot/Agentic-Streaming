package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.postgres.PostgresTestDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link PostgresChangeChannel.PostgresPollFn} against a real PostgreSQL (Testcontainers): rows are
 * emitted in cursor order, rows that share a {@code created_at} are not lost or repeated across a
 * seek, and a fresh poll fn seeked to a checkpointed position resumes strictly after it.
 */
@Tag("integration")
final class PostgresChangeChannelPollFnTest {

  private static PostgresTestDatabase database;
  private static ObjectMapper mapper;

  @BeforeAll
  static void startDatabase() throws Exception {
    database = PostgresTestDatabase.start();
    mapper = new ObjectMapper();
    mapper.registerModule(new ParameterNamesModule());
    mapper.setVisibility(
        mapper
            .getSerializationConfig()
            .getDefaultVisibilityChecker()
            .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
            .withGetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
            .withSetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
            .withCreatorVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY));
    try (Connection c = connect();
        Statement st = c.createStatement()) {
      st.execute(
          "CREATE TABLE IF NOT EXISTS agent_facts ("
              + "flow_id VARCHAR(255) NOT NULL, fact_id VARCHAR(255) NOT NULL, "
              + "fact_json TEXT NOT NULL, created_at TIMESTAMP NOT NULL, "
              + "PRIMARY KEY (flow_id, fact_id))");
    }
  }

  @AfterAll
  static void stopDatabase() {
    if (database != null) {
      database.close();
    }
  }

  @Test
  @DisplayName("seek to a checkpointed cursor resumes after the emitted row, including same-timestamp ties")
  void seekResumesAfterEmittedRow() throws Exception {
    String flow = "flow-" + UUID.randomUUID();
    long base = System.currentTimeMillis() - 60_000;
    int sameTs = 3 + ThreadLocalRandom.current().nextInt(4);
    int later = 2 + ThreadLocalRandom.current().nextInt(4);
    List<String> expected = new ArrayList<>();
    // Several rows with an identical created_at (ties are ordered by the primary key) ...
    for (int i = 0; i < sameTs; i++) {
      expected.add(insert(flow, String.format("fact-%03d", i), new Timestamp(base)));
    }
    // ... then rows with increasing timestamps.
    for (int i = 0; i < later; i++) {
      expected.add(
          insert(flow, String.format("fact-%03d", sameTs + i), new Timestamp(base + 1000L * (i + 1))));
    }
    expected.sort(String::compareTo); // fact ids were chosen so pk order == insertion order

    PostgresChangeChannel.PostgresPollFn first =
        new PostgresChangeChannel.PostgresPollFn(
            database.jdbcUrl(), user(), password(), 0L);
    first.open(0);
    first.seek(null);
    assertNull(first.position());

    int emitBeforeCheckpoint = 1 + ThreadLocalRandom.current().nextInt(sameTs - 1);
    List<String> seen = new ArrayList<>();
    for (int i = 0; i < emitBeforeCheckpoint; i++) {
      seen.add(pollContent(first));
    }
    String checkpointed = first.position();
    assertEquals(expected.subList(0, emitBeforeCheckpoint), seen);
    first.close();

    PostgresChangeChannel.PostgresPollFn restored =
        new PostgresChangeChannel.PostgresPollFn(
            database.jdbcUrl(), user(), password(), 0L);
    restored.open(0);
    restored.seek(checkpointed);
    assertEquals(checkpointed, PostgresChangeChannel.Cursor.decode(checkpointed).encode());
    List<String> rest = new ArrayList<>();
    for (int i = emitBeforeCheckpoint; i < expected.size(); i++) {
      rest.add(pollContent(restored));
    }
    assertEquals(expected.subList(emitBeforeCheckpoint, expected.size()), rest);
    assertNull(restored.poll(10), "no more rows after the last one");

    // A row inserted after the cursor is picked up on the next poll; older rows are not re-read.
    String fresh = insert(flow, "fact-zzz", new Timestamp(base + 1000L * (later + 5)));
    assertEquals(fresh, pollContent(restored));
    assertNull(restored.poll(10));
    restored.close();
  }

  private static String pollContent(PostgresChangeChannel.PostgresPollFn fn) throws Exception {
    KeyedContextItem item = fn.poll(10);
    long deadline = System.nanoTime() + 5_000_000_000L;
    while (item == null && System.nanoTime() < deadline) {
      item = fn.poll(10);
    }
    if (item == null) {
      throw new AssertionError("poll returned nothing");
    }
    return item.getItem().getContent();
  }

  private static String insert(String flow, String factId, Timestamp createdAt) throws Exception {
    String content = factId + "-" + UUID.randomUUID();
    String json =
        mapper.writeValueAsString(
            new ContextItem(content, ContextPriority.MUST, MemoryType.LONG_TERM));
    try (Connection c = connect();
        PreparedStatement ps =
            c.prepareStatement(
                "INSERT INTO agent_facts (flow_id, fact_id, fact_json, created_at) VALUES (?, ?, ?, ?)")) {
      ps.setString(1, flow);
      ps.setString(2, factId);
      ps.setString(3, json);
      ps.setTimestamp(4, createdAt);
      ps.executeUpdate();
    }
    return content;
  }

  private static String user() {
    return database.storeConfig().get("postgres.user");
  }

  private static String password() {
    return database.storeConfig().get("postgres.password");
  }

  private static Connection connect() throws Exception {
    return DriverManager.getConnection(database.jdbcUrl(), user(), password());
  }
}
