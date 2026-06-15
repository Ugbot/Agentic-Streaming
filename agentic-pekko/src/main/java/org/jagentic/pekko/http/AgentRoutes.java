package org.jagentic.pekko.http;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;
import org.apache.pekko.http.javadsl.unmarshalling.Unmarshaller;

import org.jagentic.core.Event;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.ConversationManager;

/** Pekko HTTP routes: the Agent Card and a {@code POST /agent} turn endpoint. Turns are dispatched
 * non-blockingly (ask the entity, complete with the future) so HTTP threads aren't blocked on
 * LLM/tool I/O. */
public final class AgentRoutes extends AllDirectives {

  private final ActorSystem<ConversationManager.Command> system;
  private final AgentCard card;
  private final Duration timeout;
  private final ObjectMapper mapper = new ObjectMapper();

  public AgentRoutes(ActorSystem<ConversationManager.Command> system, AgentCard card, Duration timeout) {
    this.system = system;
    this.card = card;
    this.timeout = timeout;
  }

  public Route routes() {
    return concat(
        pathPrefix(".well-known", () ->
            path("agent-card.json", () -> get(this::agentCard))),
        path("agent", () ->
            post(() -> entity(Unmarshaller.entityToString(), this::handleAgent))),
        path("healthz", () -> get(() -> complete(json("{\"status\":\"ok\"}")))));
  }

  private Route agentCard() {
    return complete(json(write(card.toJson())));
  }

  private Route handleAgent(String body) {
    JsonNode in;
    try {
      in = mapper.readTree(body);
    } catch (Exception e) {
      return complete(HttpResponse.create().withStatus(StatusCodes.BAD_REQUEST)
          .withEntity(ContentTypes.APPLICATION_JSON, "{\"error\":\"invalid json\"}"));
    }
    String cid = field(in, "c-" + UUID.randomUUID(), "conversation_id", "conversationId");
    String uid = field(in, "anonymous", "user_id", "userId");
    String text = field(in, "", "text");

    CompletionStage<ConversationEntity.TurnReply> ask = AskPattern.ask(
        system,
        replyTo -> new ConversationManager.Envelope(cid,
            new ConversationEntity.ProcessTurn(UUID.randomUUID().toString(), new Event(cid, uid, text), replyTo)),
        timeout,
        system.scheduler());

    CompletionStage<HttpResponse> response = ask.thenApply(r -> {
      Map<String, Object> out = new LinkedHashMap<>();
      out.put("conversation_id", r.conversationId());
      out.put("reply", r.reply());
      out.put("path", r.path());
      out.put("ok", r.ok());
      out.put("tool_calls", r.toolCalls());
      return json(write(out));
    });
    return completeWithFuture(response);
  }

  private HttpResponse json(String body) {
    return HttpResponse.create().withEntity(ContentTypes.APPLICATION_JSON, body);
  }

  private String write(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      return "{}";
    }
  }

  private static String field(JsonNode node, String fallback, String... keys) {
    for (String k : keys) {
      JsonNode v = node.get(k);
      if (v != null && !v.isNull()) {
        return v.asText();
      }
    }
    return fallback;
  }
}
