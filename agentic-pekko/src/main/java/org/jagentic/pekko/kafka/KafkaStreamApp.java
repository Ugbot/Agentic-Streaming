package org.jagentic.pekko.kafka;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.kafka.CommitterSettings;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.ProducerMessage;
import org.apache.pekko.kafka.ProducerSettings;
import org.apache.pekko.kafka.Subscriptions;
import org.apache.pekko.kafka.javadsl.Committer;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.kafka.javadsl.Producer;

import org.jagentic.core.Event;
import org.jagentic.pekko.runtime.ConversationManager;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.TurnWire;

/**
 * Kafka ingress/egress: a committable source of request records ({@link TurnWire} JSON) →
 * backpressured {@code mapAsync} that asks the conversation entity → the normalized result document
 * produced to the output topic, keyed by conversationId → offset commit. Delivery is
 * at-least-once; because the record's own {@code turn_id} is the entity's idempotency key, a
 * redelivered record produces a {@code duplicate} result instead of re-running the turn. A record
 * that is not a valid turn is answered with an error document (see {@link #malformed}) and
 * committed, so one bad record cannot wedge the partition.
 */
public final class KafkaStreamApp {

  private KafkaStreamApp() {}

  public static Consumer.DrainingControl<Done> run(
      ActorSystem<ConversationManager.Command> system, String bootstrap, String inTopic,
      String outTopic, String groupId, int parallelism, Duration timeout) {

    ConsumerSettings<String, String> consumerSettings =
        ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
            .withBootstrapServers(bootstrap)
            .withGroupId(groupId)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    ProducerSettings<String, String> producerSettings =
        ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
            .withBootstrapServers(bootstrap);

    return Consumer.committableSource(consumerSettings, Subscriptions.topics(inTopic))
        .mapAsync(parallelism, msg -> answer(system, msg.record(), outTopic, timeout)
            .thenApply(record -> ProducerMessage.single(record, msg.committableOffset())))
        .via(Producer.flexiFlow(producerSettings))
        .map(results -> results.passThrough())
        .toMat(Committer.sink(CommitterSettings.create(system)), Consumer::createDrainingControl)
        .run(system);
  }

  /** One request record → one result record. Exposed so the mapping is testable without a broker. */
  public static CompletionStage<ProducerRecord<String, String>> answer(
      ActorSystem<ConversationManager.Command> system, ConsumerRecord<String, String> in,
      String outTopic, Duration timeout) {
    Event event;
    try {
      event = TurnWire.parse(in.value());
    } catch (TurnWire.MalformedTurn e) {
      return CompletableFuture.completedFuture(
          new ProducerRecord<>(outTopic, in.key(), TurnWire.write(malformed(in, e))));
    }
    return PekkoRuntime.ask(system, event, timeout)
        .thenApply(reply -> new ProducerRecord<>(outTopic, reply.conversationId(), TurnWire.write(reply)));
  }

  static Map<String, Object> malformed(ConsumerRecord<String, String> in, TurnWire.MalformedTurn e) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("error", Map.of("error_class", "malformed_record", "message", e.getMessage()));
    out.put("topic", in.topic());
    out.put("partition", in.partition());
    out.put("offset", in.offset());
    out.put("key", in.key());
    return out;
  }
}
