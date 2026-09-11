package org.jagentic.pekko.http;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;
import org.apache.pekko.http.javadsl.unmarshalling.Unmarshaller;

import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.ConversationManager;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.TurnWire;

/**
 * Pekko HTTP routes: the Agent Card, {@code POST /agent} (one turn or resume signal in the
 * {@link TurnWire} format, answered with the normalized result document) and
 * {@code GET /conversations/{id}} (the folded state and journal). Turns are dispatched
 * non-blockingly so HTTP threads never wait on LLM/tool I/O.
 */
public final class AgentRoutes extends AllDirectives {

  private final ActorSystem<ConversationManager.Command> system;
  private final AgentCard card;
  private final Duration timeout;

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
        pathPrefix("conversations", () ->
            path(cid -> get(() -> conversation(cid)))),
        path("healthz", () -> get(() -> complete(json("{\"status\":\"ok\"}")))));
  }

  private Route agentCard() {
    return complete(json(TurnWire.write(card.toJson())));
  }

  private Route handleAgent(String body) {
    Event event;
    try {
      event = TurnWire.parse(body);
    } catch (TurnWire.MalformedTurn e) {
      return complete(HttpResponse.create().withStatus(StatusCodes.BAD_REQUEST)
          .withEntity(ContentTypes.APPLICATION_JSON, TurnWire.write(Map.of("error", e.getMessage()))));
    }
    CompletionStage<HttpResponse> response = PekkoRuntime.ask(system, event, timeout)
        .thenApply(r -> json(TurnWire.write(r)));
    return completeWithFuture(response);
  }

  private Route conversation(String conversationId) {
    CompletionStage<ConversationEntity.StateSnapshot> ask = AskPattern.ask(
        system,
        (ActorRef<ConversationEntity.StateSnapshot> replyTo) -> new ConversationManager.Envelope(conversationId,
            new ConversationEntity.GetState(replyTo)),
        timeout,
        system.scheduler());
    return completeWithFuture(ask.thenApply(s -> {
      Map<String, Object> out = new LinkedHashMap<>();
      out.put("conversation_id", s.conversationId());
      out.put("state", s.reduced());
      out.put("suspended", s.suspendedTurnIds().stream().sorted().toList());
      out.put("pending_timers", s.pendingTimers().keySet().stream().sorted().toList());
      out.put("events", s.events().stream().map(LogEvent::toMap).toList());
      return json(TurnWire.write(out));
    }));
  }

  private HttpResponse json(String body) {
    return HttpResponse.create().withEntity(ContentTypes.APPLICATION_JSON, body);
  }
}
