package org.jagentic.pekko.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
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
import org.jagentic.core.TurnError;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.identity.Identity;
import org.jagentic.core.identity.PrincipalSeal;
import org.jagentic.pekko.runtime.ConversationManager;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.TurnWire;

/**
 * Kafka ingress/egress: a committable source of request records ({@link TurnWire} JSON) →
 * backpressured {@code mapAsync} that asks the conversation entity → the normalized result document
 * produced to the output topic, keyed by conversationId → offset commit. Delivery is
 * at-least-once; the record's {@code turn_id} is the entity's idempotency key, so a redelivered
 * record produces a {@code duplicate} result instead of re-running the turn. The id is resolved by
 * {@link #turnId}: the payload's {@code turn_id}, else a {@value #TURN_ID_HEADER} header, else
 * {@code topic-partition-offset}, which is the same for every redelivery of one record and so
 * dedupes even when the producer never set an id. A record that is not a valid turn is answered
 * with an error document (see {@link #malformed}) and committed, so one bad record cannot wedge the
 * partition.
 *
 * <p>{@link #run} will not start until told who may claim a principal: {@code AGENTIC_PRINCIPAL_KEYS}
 * (the gateway's public keys; a record's {@code principal_proof} must verify) or {@code
 * AGENTIC_IDENTITY=unauthenticated} (the record is believed, and the log says so). Kafka ACLs on the
 * input topic are needed either way.
 */
public final class KafkaStreamApp {

  /** Record header consulted for the turn id when the JSON payload has none. */
  public static final String TURN_ID_HEADER = "turn_id";

  private KafkaStreamApp() {}

  public static Consumer.DrainingControl<Done> run(
      ActorSystem<ConversationManager.Command> system, String bootstrap, String inTopic,
      String outTopic, String groupId, int parallelism, Duration timeout) {

    ConsumerSettings<String, String> consumerSettings =
        ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
            .withBootstrapServers(bootstrap)
            .withGroupId(groupId)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    PrincipalSeal.Verifier keys = identity(System.getenv(), inTopic);
    ProducerSettings<String, String> producerSettings =
        ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
            .withBootstrapServers(bootstrap);

    return Consumer.committableSource(consumerSettings, Subscriptions.topics(inTopic))
        .mapAsync(parallelism, msg -> answer(system, msg.record(), outTopic, timeout, keys)
            .thenApply(record -> ProducerMessage.single(record, msg.committableOffset())))
        .via(Producer.flexiFlow(producerSettings))
        .map(results -> results.passThrough())
        .toMat(Committer.sink(CommitterSettings.create(system)), Consumer::createDrainingControl)
        .run(system);
  }

  /**
   * Who may claim a principal on {@code topic}: the gateway's public keys from {@code
   * AGENTIC_PRINCIPAL_KEYS}, or null with {@code AGENTIC_IDENTITY=unauthenticated}, which is logged.
   * With neither the front door does not start.
   */
  static PrincipalSeal.Verifier identity(Map<String, String> env, String topic) {
    return Identity.forFrontDoor(env, topic);
  }

  /**
   * As {@link #answer(ActorSystem, ConsumerRecord, String, Duration)}, believing a record's principal
   * only with the gateway's proof for that turn when {@code keys} is set. Anything else is answered
   * as a turn failed with class policy, the entity is not asked, and the stream carries on.
   */
  public static CompletionStage<ProducerRecord<String, String>> answer(
      ActorSystem<ConversationManager.Command> system, ConsumerRecord<String, String> in,
      String outTopic, Duration timeout, PrincipalSeal.Verifier keys) {
    Event event;
    try {
      event = TurnWire.parseVouched(in.value(), () -> turnId(in));
    } catch (TurnWire.MalformedTurn e) {
      return CompletableFuture.completedFuture(
          new ProducerRecord<>(outTopic, in.key(), TurnWire.write(malformed(in, e))));
    }
    if (keys != null) {
      String problem = keys.problem(event, java.time.Instant.now(), false);
      if (problem != null) {
        Map<String, Object> refused = new TurnResult(event.conversationId(), event.turnId(), TurnStatus.FAILED,
            null, null, new TurnError(TurnError.ErrorClass.POLICY, problem), List.of(), List.of(), Map.of()).toMap();
        return CompletableFuture.completedFuture(
            new ProducerRecord<>(outTopic, event.conversationId(), TurnWire.write(refused)));
      }
    }
    return PekkoRuntime.ask(system, event, timeout)
        .thenApply(reply -> new ProducerRecord<>(outTopic, reply.conversationId(), TurnWire.write(reply)));
  }

  /** One request record → one result record. Exposed so the mapping is testable without a broker. */
  public static CompletionStage<ProducerRecord<String, String>> answer(
      ActorSystem<ConversationManager.Command> system, ConsumerRecord<String, String> in,
      String outTopic, Duration timeout) {
    return answer(system, in, outTopic, timeout, null);
  }

  /**
   * The turn id a record carries outside its payload: the {@value #TURN_ID_HEADER} header when
   * present and non-blank, else {@code topic-partition-offset}. Deterministic per record, so the
   * same record redelivered after a crash or rebalance resolves to the same id.
   */
  public static String turnId(ConsumerRecord<String, String> in) {
    Header h = in.headers() == null ? null : in.headers().lastHeader(TURN_ID_HEADER);
    if (h != null && h.value() != null) {
      String v = new String(h.value(), StandardCharsets.UTF_8).trim();
      if (!v.isEmpty()) {
        return v;
      }
    }
    return in.topic() + "-" + in.partition() + "-" + in.offset();
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
