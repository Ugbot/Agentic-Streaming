package org.jagentic.pekko.durability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.testing.CountingGraph;

/**
 * Persistence profile selection. Every profile is an event-sourced journal; selecting one whose
 * connection is not configured fails at boot instead of silently falling back to memory.
 *
 * <p>The Postgres round trip runs when {@code AGENTIC_PEKKO_INTEGRATION=true}; it then requires
 * {@code AGENTIC_PG_URL} (plus user/password) and fails — never skips — if the database is missing.</p>
 */
class DurabilityProfileTest {

  @Test
  void namesResolveCaseInsensitivelyAndBlankMeansMemory() {
    assertEquals(DurabilityProfile.MEMORY, DurabilityProfile.from(null));
    assertEquals(DurabilityProfile.MEMORY, DurabilityProfile.from("  "));
    assertEquals(DurabilityProfile.POSTGRES, DurabilityProfile.from("postgres"));
    assertEquals(DurabilityProfile.CASSANDRA, DurabilityProfile.from(" Cassandra "));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> DurabilityProfile.from("redis-" + UUID.randomUUID()));
    assertTrue(e.getMessage().contains("MEMORY"));
    assertEquals(List.of(DurabilityProfile.MEMORY, DurabilityProfile.POSTGRES, DurabilityProfile.CASSANDRA),
        List.of(DurabilityProfile.values()), "only event-sourced journal profiles exist");
  }

  @Test
  void memoryProfileSelectsTheInMemoryJournalAndBootsWithIt() {
    Config c = DurabilityProfile.MEMORY.config();
    assertEquals("pekko.persistence.journal.inmem", c.getString("pekko.persistence.journal.plugin"));
    assertEquals("jackson-cbor", c.getString(
        "pekko.actor.serialization-bindings.\"org.jagentic.pekko.serialization.CborSerializable\""));
    try (PekkoSystem sys = new PekkoSystem(new CountingGraph().deps(), DurabilityProfile.MEMORY)) {
      assertEquals(DurabilityProfile.MEMORY, sys.profile());
      assertEquals(DurabilityProfile.MEMORY.journalPlugin(), sys.journalPlugin());
    }
  }

  @Test
  void postgresProfileRefusesToStartWithoutAConnection() {
    if (System.getenv("AGENTIC_PG_URL") != null) {
      Config c = DurabilityProfile.POSTGRES.config();
      assertEquals("jdbc-journal", c.getString("pekko.persistence.journal.plugin"));
      return;
    }
    IllegalStateException e = assertThrows(IllegalStateException.class, DurabilityProfile.POSTGRES::config);
    assertTrue(e.getMessage().contains("AGENTIC_PG_URL"), e.getMessage());
  }

  @Test
  void postgresProfileResolvesTheJdbcJournalWhenTheUrlIsSupplied() {
    String key = "pekko.persistence.jdbc.shared-databases.default.db.url";
    String url = "jdbc:postgresql://db-" + UUID.randomUUID().toString().substring(0, 6) + ":5432/agentic";
    System.setProperty(key, url);
    ConfigFactory.invalidateCaches();
    try {
      Config c = DurabilityProfile.POSTGRES.config();
      assertEquals("jdbc-journal", c.getString("pekko.persistence.journal.plugin"));
      assertEquals("jdbc-snapshot-store", c.getString("pekko.persistence.snapshot-store.plugin"));
      assertEquals(url, c.getString(key));
      assertEquals("cluster", c.getString("pekko.actor.provider"));
    } finally {
      System.clearProperty(key);
      ConfigFactory.invalidateCaches();
    }
  }

  @Test
  void cassandraProfileSelectsTheCassandraJournal() {
    Config c = DurabilityProfile.CASSANDRA.config();
    assertEquals("pekko.persistence.cassandra.journal", c.getString("pekko.persistence.journal.plugin"));
    assertTrue(c.hasPath("datastax-java-driver.basic.contact-points"));
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "AGENTIC_PEKKO_INTEGRATION", matches = "true")
  void postgresJournalSurvivesEntityRestart() {
    if (System.getenv("AGENTIC_PG_URL") == null) {
      fail("AGENTIC_PEKKO_INTEGRATION=true but AGENTIC_PG_URL is not set: Postgres is required for this test");
    }
    CountingGraph graph = new CountingGraph();
    String cid = "pg-" + UUID.randomUUID();
    String t1 = "t-" + UUID.randomUUID();
    try (PekkoSystem sys = new PekkoSystem(graph.deps(), DurabilityProfile.POSTGRES);
         PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(30))) {
      assertEquals("jdbc-journal", sys.journalPlugin());
      TurnResult r = rt.submit(Event.turn(cid, t1, "lee", "what is my balance?"));
      assertEquals(TurnStatus.COMPLETED, r.status);
      rt.passivate(cid);
      assertEquals(r.events, rt.state(cid).events());
      TurnResult dup = rt.submit(Event.turn(cid, t1, "lee", "what is my balance?"));
      assertEquals(TurnStatus.DUPLICATE, dup.status);
      assertNotNull(dup.reply);
      assertEquals(1, graph.brainCalls.get());
    }
  }
}
