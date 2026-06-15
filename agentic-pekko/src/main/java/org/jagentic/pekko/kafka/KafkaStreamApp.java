package org.jagentic.pekko.kafka;

import java.time.Duration;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.kafka.CommitterSettings;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.ProducerMessage;
import org.apache.pekko.kafka.ProducerSettings;
import org.apache.pekko.kafka.Subscriptions;
import org.apache.pekko.kafka.javadsl.Committer;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.kafka.javadsl.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.jagentic.core.Event;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.ConversationManager;

/** Kafka ingress/egress: a committable source of request records → backpressured {@code mapAsync}
 * that asks the conversation entity → produce the reply to the output topic and commit the offset
 * (at-least-once; the entity's turnId dedupe makes it effectively-once on the conversation view).
 * Replies are keyed by conversationId so egress preserves per-conversation order. */
public final class KafkaStreamApp {

  private static final ObjectMapper MAPPER = new ObjectMapper();

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
        .mapAsync(parallelism, msg -> {
          Event event = parse(msg.record().value());
          return AskPattern.ask(
                  system,
                  (ActorRef<ConversationEntity.TurnReply> replyTo) -> new ConversationManager.Envelope(
                      event.conversationId(),
                      new ConversationEntity.ProcessTurn(UUID.randomUUID().toString(), event, replyTo)),
                  timeout,
                  system.scheduler())
              .thenApply(reply -> ProducerMessage.single(
                  new ProducerRecord<>(outTopic, reply.conversationId(), write(reply)),
                  msg.committableOffset()));
        })
        .via(Producer.flexiFlow(producerSettings))
        .map(results -> results.passThrough())
        .toMat(Committer.sink(CommitterSettings.create(system)), Consumer::createDrainingControl)
        .run(system);
  }

  private static Event parse(String json) {
    try {
      JsonNode n = MAPPER.readTree(json);
      String cid = text(n, "conversation_id", "conversationId", "c-" + UUID.randomUUID());
      String uid = text(n, "user_id", "userId", "anonymous");
      String txt = text(n, "text", "text", "");
      return new Event(cid, uid, txt);
    } catch (Exception e) {
      return new Event("c-" + UUID.randomUUID(), "anonymous", json);
    }
  }

  private static String text(JsonNode node, String k1, String k2, String fallback) {
    if (node.hasNonNull(k1)) {
      return node.get(k1).asText();
    }
    if (node.hasNonNull(k2)) {
      return node.get(k2).asText();
    }
    return fallback;
  }

  private static String write(ConversationEntity.TurnReply r) {
    try {
      return MAPPER.writeValueAsString(java.util.Map.of(
          "conversation_id", r.conversationId(), "reply", r.reply(),
          "path", r.path() == null ? "" : r.path(), "ok", r.ok()));
    } catch (Exception e) {
      return "{}";
    }
  }
}
