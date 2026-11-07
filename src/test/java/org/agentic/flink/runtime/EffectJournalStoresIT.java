package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.jagentic.core.EffectJournal;
import org.jagentic.core.ToolEffect;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.store.KafkaEffectJournal;
import org.jagentic.core.store.PostgresEffectJournal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * The durable journals against a real Postgres and a real Kafka broker in containers this test
 * starts: an entry outlives the object that wrote it, and the double payment stays fixed when the
 * journal is one of them. Runs under {@code -P integration-tests}.
 */
@Tag("integration")
class EffectJournalStoresIT {
  private static final EffectJournal.Key KEY =
      new EffectJournal.Key("c", "t", EffectJournal.TOOL, 0);

  private static GenericContainer<?> postgres;
  private static GenericContainer<?> kafka;
  private static String bootstrap;

  @BeforeAll
  @SuppressWarnings("deprecation")
  static void start() throws Exception {
    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withEnv("POSTGRES_PASSWORD", "journal")
            .withExposedPorts(5432)
            .waitingFor(Wait.forLogMessage(".*ready to accept connections.*\\n", 2));
    postgres.start();
    int port;
    try (ServerSocket free = new ServerSocket(0)) {
      port = free.getLocalPort();
    }
    bootstrap = "localhost:" + port;
    kafka =
        new FixedHostPortGenericContainer<>("apache/kafka:4.1.0")
            .withFixedExposedPort(port, 9092)
            .withEnv("KAFKA_NODE_ID", "1")
            .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://" + bootstrap)
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .waitingFor(Wait.forLogMessage(".*Kafka Server started.*\\n", 1));
    kafka.start();
  }

  @AfterAll
  static void stop() {
    postgres.stop();
    kafka.stop();
  }

  private static String jdbcUrl() {
    return "jdbc:postgresql://"
        + postgres.getHost()
        + ":"
        + postgres.getMappedPort(5432)
        + "/postgres";
  }

  private static EffectJournal postgres() {
    return new PostgresEffectJournal(jdbcUrl(), "postgres", "journal");
  }

  private static void outlivesItsWriter(
      EffectJournal first, java.util.function.Supplier<EffectJournal> reopen) {
    EffectJournal.Entry pending = EffectJournal.Entry.pending(KEY, "pay:1", ToolEffect.IDEMPOTENT);
    assertTrue(first.putIfAbsent(pending).isEmpty(), "nothing was there");
    assertEquals(EffectJournal.State.PENDING, first.putIfAbsent(pending).orElseThrow().state());
    first.put(pending.withAttempt(1, "boom").succeeded(Map.of("paid", true)));
    first.close();
    try (EffectJournal second = reopen.get()) {
      EffectJournal.Entry read = second.get(KEY).orElseThrow();
      assertEquals(EffectJournal.State.SUCCEEDED, read.state());
      assertEquals(Map.of("paid", true), read.result());
      assertEquals(List.of(new EffectJournal.Attempt(1, "boom")), read.attempts());
      assertFalse(second.get(new EffectJournal.Key("c", "t", EffectJournal.TOOL, 1)).isPresent());
    }
  }

  @Test
  void postgresEntryOutlivesItsWriter() {
    outlivesItsWriter(postgres(), EffectJournalStoresIT::postgres);
  }

  @Test
  void kafkaEntryOutlivesItsWriterOnACompactedTopic() throws Exception {
    String topic = "effects-" + UUID.randomUUID();
    outlivesItsWriter(
        new KafkaEffectJournal(bootstrap, topic), () -> new KafkaEffectJournal(bootstrap, topic));
    Properties p = new Properties();
    p.put("bootstrap.servers", bootstrap);
    try (Admin admin = Admin.create(p)) {
      ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
      assertEquals("compact", config.get("cleanup.policy").value());
    }
  }

  private static void paysOnce(String journal) throws Exception {
    PayAfterRestoreTest bank = new PayAfterRestoreTest();
    bank.startTheBank();
    try {
      TurnResult r = bank.crashAndRedeliver("non_idempotent", "after", journal);
      assertEquals(TurnStatus.COMPLETED, r.status);
      assertEquals(1, bank.keys.size(), "the bank saw " + bank.keys);
    } finally {
      bank.stopTheBank();
    }
  }

  @Test
  void aPaymentAcrossRestoreHitsTheBankOnceWithAPostgresJournal() throws Exception {
    paysOnce(
        "{\"kind\": \"postgres\", \"jdbc_url\": \"%s\", \"user\": \"postgres\", \"password\": \"journal\"}"
            .formatted(jdbcUrl()));
  }

  @Test
  void aPaymentAcrossRestoreHitsTheBankOnceWithAKafkaJournal() throws Exception {
    paysOnce(
        "{\"kind\": \"kafka\", \"url\": \"%s\", \"topic\": \"effects-%s\"}"
            .formatted(bootstrap, UUID.randomUUID()));
  }
}
