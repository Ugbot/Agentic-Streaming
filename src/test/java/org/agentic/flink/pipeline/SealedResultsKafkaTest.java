package org.agentic.flink.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.agentic.flink.testkit.RepoRoot;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jagentic.core.LogEvent;
import org.jagentic.core.privacy.FileKeyVault;
import org.jagentic.gateway.KafkaBackend;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/**
 * Crypto-shredding did not reach the results topic: a result carried the turn's text and reply in
 * the open, and the gateway's view of a conversation was rebuilt from those results, so destroying
 * a conversation's key left everything readable there. With a key vault, the job seals each result
 * under the conversation's key, and the gateway opens it only when read. After {@code Erase}, the
 * records on the topic are ciphertext nobody can open and the gateway shows nothing. On a real
 * broker ({@code AGENTIC_KAFKA_BOOTSTRAP}, which CI provides).
 */
@EnabledIfEnvironmentVariable(named = "AGENTIC_KAFKA_BOOTSTRAP", matches = ".+")
class SealedResultsKafkaTest {

  private static final String TEXT = "what is my balance?";

  @Test
  void anErasedConversationCannotBeReadFromTheResultsTopicOrTheGateway(@TempDir Path vault)
      throws Exception {
    String bootstrap = System.getenv("AGENTIC_KAFKA_BOOTSTRAP");
    String input = "sealed-" + UUID.randomUUID();
    String results = input + ".results";
    String cid = "acme/c-" + UUID.randomUUID();
    System.setProperty("agentic.key_vault_dir", vault.toString());
    try {
      try (KafkaProducer<String, String> producer = producer(bootstrap)) {
        for (String turn : List.of("t1", "t2")) {
          producer
              .send(
                  new ProducerRecord<>(
                      input,
                      cid,
                      "{\"conversation_id\":\""
                          + cid
                          + "\",\"turn_id\":\""
                          + turn
                          + "\",\"text\":\""
                          + TEXT
                          + "\",\"principal\":{\"subject\":\"alice\",\"tenant\":\"acme\"}}"))
              .get();
        }
      }
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      KafkaPipelineJob.assemble(
          env,
          FlinkPipelineRunner.loadYaml(RepoRoot.examplePipeline("banking.yaml")),
          new KafkaPipelineJob.Options(
              bootstrap, input, results, results + ".failures", "g-" + UUID.randomUUID()),
          Map.of("AGENTIC_IDENTITY", "unauthenticated"));
      JobClient job = env.executeAsync("sealed-results");
      List<String> raw;
      try {
        raw = readUntil(bootstrap, results, 2);
      } finally {
        job.cancel().get();
        try {
          job.getJobExecutionResult().get(); // its local cluster is gone before the next job starts
        } catch (java.util.concurrent.ExecutionException cancelled) {
          // a cancelled job ends this way
        }
      }
      assertEquals(2, raw.size(), raw.toString());

      try (KafkaBackend gateway = KafkaBackend.connect(bootstrap, input, results)) {
        List<LogEvent> before = waitFor(gateway, cid, true);
        assertTrue(before.toString().contains(TEXT), "the owner reads the conversation: " + before);
        assertEquals(1, new FileKeyVault(vault).erase("acme/alice"));
        assertEquals(
            List.of(),
            gateway.conversation(cid).toCompletableFuture().get(),
            "the gateway's view is gone with the key");
      }
      for (String record : raw) {
        assertFalse(record.contains(TEXT), "the topic holds the turn's text: " + record);
        assertFalse(record.contains("1234.56"), "the topic holds the reply: " + record);
      }
      try (KafkaBackend later = KafkaBackend.connect(bootstrap, input, results)) {
        Thread.sleep(2000);
        assertEquals(List.of(), later.conversation(cid).toCompletableFuture().get());
      }
    } finally {
      System.clearProperty("agentic.key_vault_dir");
    }
  }

  private static List<LogEvent> waitFor(KafkaBackend gateway, String cid, boolean any)
      throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      List<LogEvent> events = gateway.conversation(cid).toCompletableFuture().get();
      if (!events.isEmpty() == any) {
        return events;
      }
      Thread.sleep(200);
    }
    throw new AssertionError("the gateway never saw " + cid);
  }

  private static KafkaProducer<String, String> producer(String bootstrap) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    return new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
  }

  private static List<String> readUntil(String bootstrap, String topic, int n) throws Exception {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    List<String> out = new ArrayList<>();
    try (KafkaConsumer<String, String> c =
        new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
      long deadline = System.currentTimeMillis() + 90_000;
      List<TopicPartition> parts = List.of();
      while (parts.isEmpty() && System.currentTimeMillis() < deadline) {
        parts =
            c.partitionsFor(topic).stream()
                .map(i -> new TopicPartition(topic, i.partition()))
                .toList();
        Thread.sleep(200);
      }
      c.assign(parts);
      c.seekToBeginning(parts);
      while (out.size() < n && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
          out.add(r.value());
        }
      }
    }
    return out;
  }
}
