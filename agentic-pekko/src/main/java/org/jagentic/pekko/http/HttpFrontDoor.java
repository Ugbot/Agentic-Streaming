package org.jagentic.pekko.http;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;

import org.jagentic.pekko.runtime.ConversationManager;

/** Binds the {@link AgentRoutes} to an HTTP host:port over the conversation actor system. */
public final class HttpFrontDoor {

  private HttpFrontDoor() {}

  public static CompletionStage<ServerBinding> start(
      ActorSystem<ConversationManager.Command> system, String host, int port,
      AgentCard card, Duration askTimeout) {
    AgentRoutes routes = new AgentRoutes(system, card, askTimeout);
    return Http.get(system).newServerAt(host, port).bind(routes.routes());
  }
}
