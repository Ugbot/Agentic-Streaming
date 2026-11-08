package org.jagentic.gateway;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;

/**
 * A Flink job between Kafka topics ({@code KafkaPipelineJob}): a turn goes to the input topic, keyed
 * by conversation, with the principal the gateway verified, and the answer comes back on the results
 * topic. The results topic read from its start is also how a conversation is read, so a conversation
 * shows no further back than that topic's retention.
 *
 * <p>With {@code AGENTIC_PRINCIPAL_SIGNING_KEY} each record carries {@code principal_proof}, the
 * gateway's signature over its principal and that turn, and a job given the public key believes no
 * principal without one. Without it, whatever can write the input topic can claim any principal, so
 * Kafka ACLs on that topic are required either way. A turn the job has not answered in time is a 504, and redelivering the same
 * {@code turn_id} is safe.
 */
public final class KafkaBackend implements TurnBackend, AutoCloseable {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};

  private final Producer<String, byte[]> producer;
  private final Consumer<String, byte[]> consumer;
  private final String inputTopic;
  private final org.jagentic.core.privacy.KeyVault vault = org.jagentic.core.privacy.Vaults.fromEnvironment();
  private final ConversationView view = new ConversationView(vault);
  private final Map<String, CompletableFuture<TurnResult>> waiting = new ConcurrentHashMap<>();
  private final Thread reader;
  private volatile boolean running = true;

  /** Reads {@code resultsTopic} from its start, on every partition, with no consumer group. */
  public static KafkaBackend connect(String bootstrap, String inputTopic, String resultsTopic) {
    Properties out = new Properties();
    out.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    out.put(ProducerConfig.ACKS_CONFIG, "all");
    Producer<String, byte[]> producer =
        new KafkaProducer<>(out, new StringSerializer(), new ByteArraySerializer());
    Properties in = new Properties();
    in.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    in.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    Consumer<String, byte[]> consumer =
        new KafkaConsumer<>(in, new StringDeserializer(), new ByteArrayDeserializer());
    List<TopicPartition> partitions = consumer.partitionsFor(resultsTopic).stream()
        .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
    consumer.assign(partitions);
    consumer.seekToBeginning(partitions);
    return new KafkaBackend(producer, consumer, inputTopic);
  }

  KafkaBackend(Producer<String, byte[]> producer, Consumer<String, byte[]> consumer, String inputTopic) {
    this.producer = producer;
    this.consumer = consumer;
    this.inputTopic = inputTopic;
    this.reader = Thread.ofPlatform().daemon().name("gateway-results").start(this::read);
  }

  private void read() {
    try {
      while (running) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
          accept(record.value());
        }
      }
    } catch (WakeupException e) {
      if (running) {
        throw e;
      }
    }
  }

  private void accept(byte[] value) {
    TurnResult result;
    try {
      Map<String, Object> record = JSON.readValue(value, OBJECT);
      if (org.jagentic.core.privacy.SealedResults.isSealed(record)) {
        // Kept sealed in the view; what the caller waiting for it gets is opened once, now.
        view.addSealed(String.valueOf(record.get("conversation_id")), record);
        result = org.jagentic.core.privacy.SealedResults.open(vault, record)
            .map(ConversationView.TurnResults::decode)
            .orElseGet(() -> TurnResult.fromMap(record));
      } else {
        result = TurnResult.fromMap(record);
        view.add(result);
      }
    } catch (IOException | RuntimeException notAResult) {
      return;
    }
    CompletableFuture<TurnResult> answer = waiting.remove(waitKey(result.conversationId, result.turnId));
    if (answer != null) {
      answer.complete(result);
    }
  }

  private static String waitKey(String key, String turnId) {
    return key + "\u0000" + turnId;
  }

  @Override
  public CompletionStage<TurnResult> submit(Event event) {
    String wait = waitKey(event.conversationId(), event.turnId());
    CompletableFuture<TurnResult> answer = new CompletableFuture<>();
    waiting.put(wait, answer);
    try {
      producer.send(new ProducerRecord<>(inputTopic, event.conversationId(), JSON.writeValueAsBytes(record(event))),
          (meta, failed) -> {
            if (failed != null) {
              waiting.remove(wait);
              answer.completeExceptionally(failed);
            }
          });
    } catch (IOException e) {
      waiting.remove(wait);
      answer.completeExceptionally(e);
    }
    return answer.whenComplete((r, e) -> waiting.remove(wait));
  }

  private static Map<String, Object> record(Event event) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("conversation_id", event.conversationId());
    m.put("turn_id", event.turnId());
    m.put("user_id", event.userId());
    m.put("text", event.text());
    m.put("metadata", event.metadata());
    if (event.signal() != null) {
      m.put("signal", event.signal());
    }
    if (event.principal() != null) {
      m.put("principal", event.principal().toMap());
    }
    if (event.principalProof() != null) {
      m.put("principal_proof", event.principalProof());
    }
    return m;
  }

  @Override
  public CompletionStage<List<LogEvent>> conversation(String key) {
    return CompletableFuture.completedFuture(view.events(key));
  }

  @Override
  public void close() throws InterruptedException {
    running = false;
    consumer.wakeup();
    reader.join(5000);
    consumer.close();
    producer.close();
  }
}
