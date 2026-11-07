package org.jagentic.core.store;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.jagentic.core.EffectJournal;
import org.jagentic.core.EffectJournals;

/**
 * The journal as a compacted topic, keyed by the call, so the last write per call is what survives.
 * A write waits for {@code acks=all}. Opening reads the topic to its end into a map, and every read
 * first catches up to the topic's end, so a conversation that moved here from another process, as
 * after a rescale or a shard rebalance, finds what was written there.
 *
 * <p>A topic has no compare-and-set, so two live writers on one key are not excluded the way a
 * table's primary key excludes them. A runtime's single writer per conversation is what guards a
 * key; a process that has been replaced but is still running can race its replacement.
 *
 * <p>Every instance holds the whole topic, which costs memory in proportion to the entries retained.
 * Retention is the topic's: entries must outlast the longest rollback or redelivery a deployment
 * allows. The shape follows Flink Agents' Kafka action state store, in design only: a keyed upsert
 * rather than its offset replay and recovery markers, since entries are small and keyed.
 */
public final class KafkaEffectJournal implements EffectJournal {
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private final String topic;
  private final KafkaProducer<String, String> producer;
  private final KafkaConsumer<String, String> consumer;
  private final List<TopicPartition> partitions = new ArrayList<>();
  private final Map<String, Entry> entries = new ConcurrentHashMap<>();

  public KafkaEffectJournal(String bootstrap, String topic) {
    this.topic = topic;
    KafkaProducer<String, String> writer = null;
    KafkaConsumer<String, String> reader = null;
    try {
      ensureCompactedTopic(bootstrap);
      Properties p = new Properties();
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      p.put(ProducerConfig.ACKS_CONFIG, "all");
      p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (int) TIMEOUT.toMillis());
      p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis() / 3);
      p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis());
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      writer = new KafkaProducer<>(p);
      reader = open(bootstrap);
      this.producer = writer;
      this.consumer = reader;
      catchUp();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      closeQuietly(writer, reader);
      throw new Unavailable("interrupted opening the effect journal", e);
    } catch (ExecutionException | RuntimeException e) {
      closeQuietly(writer, reader);
      throw new Unavailable("cannot open the effect journal on topic " + topic, e);
    }
  }

  private static void closeQuietly(KafkaProducer<String, String> writer, KafkaConsumer<String, String> reader) {
    if (writer != null) {
      writer.close(Duration.ZERO);
    }
    if (reader != null) {
      reader.close(Duration.ZERO);
    }
  }

  private void ensureCompactedTopic(String bootstrap) throws InterruptedException, ExecutionException {
    Properties p = new Properties();
    p.put("bootstrap.servers", bootstrap);
    try (Admin admin = Admin.create(p)) {
      NewTopic wanted = new NewTopic(topic, Optional.empty(), Optional.empty())
          .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));
      try {
        admin.createTopics(List.of(wanted)).all().get();
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof TopicExistsException)) {
          throw e;
        }
      }
    }
  }

  private KafkaConsumer<String, String> open(String bootstrap) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    KafkaConsumer<String, String> reader = new KafkaConsumer<>(p);
    for (PartitionInfo info : reader.partitionsFor(topic, TIMEOUT)) {
      partitions.add(new TopicPartition(topic, info.partition()));
    }
    reader.assign(partitions);
    reader.seekToBeginning(partitions);
    return reader;
  }

  /** Reads to the end the topic has now. A store that cannot be read is {@link Unavailable}. */
  private synchronized void catchUp() {
    try {
      Map<TopicPartition, Long> end = consumer.endOffsets(partitions, TIMEOUT);
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
        if (System.nanoTime() > deadline) {
          throw new Unavailable("timed out reading the effect journal topic " + topic, null);
        }
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
          if (r.value() == null) {
            entries.remove(r.key());
          } else {
            entries.put(r.key(), EffectJournals.fromJson(r.value()));
          }
        }
      }
    } catch (Unavailable e) {
      throw e;
    } catch (KafkaException e) {
      throw new Unavailable("cannot read the effect journal topic " + topic, e);
    }
  }

  @Override
  public Optional<Entry> get(Key key) {
    catchUp();
    return Optional.ofNullable(entries.get(key.asString()));
  }

  @Override
  public void put(Entry entry) {
    String key = entry.key().asString();
    try {
      producer.send(new ProducerRecord<>(topic, key, EffectJournals.toJson(entry)))
          .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new Unavailable("interrupted writing the effect journal", e);
    } catch (ExecutionException | TimeoutException | KafkaException e) {
      throw new Unavailable("cannot write the effect journal topic " + topic, e);
    }
    entries.put(key, entry);
  }

  @Override
  public Optional<Entry> putIfAbsent(Entry entry) {
    catchUp();
    Entry existing = entries.get(entry.key().asString());
    if (existing != null) {
      return Optional.of(existing);
    }
    put(entry);
    return Optional.empty();
  }

  @Override
  public synchronized void close() {
    producer.close(TIMEOUT);
    consumer.close(TIMEOUT);
  }
}
