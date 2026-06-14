package org.jagentic.ports.pekko;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.jagentic.core.Event;

/**
 * Runnable single-node demo of the Pekko port — no cluster/broker needed. A guardian
 * spawns one {@link ConversationActor} child per conversationId (the local stand-in
 * for Cluster Sharding) and routes each turn to it; the actor's mailbox gives
 * single-writer-per-conversation, its fields hold the keyed state. Same banking
 * router->path->verifier as every other port.
 *
 *   mvn -f ports/pekko/pom.xml -q compile exec:java
 *
 * In production swap the guardian for {@link BankingSharding} (Cluster Sharding) so
 * entities distribute across the cluster, and an EventSourcedBehavior for durability.
 */
public final class LocalDemo {
  private LocalDemo() {}

  /** Guardian protocol: route a turn to (get-or-spawn) the conversation's actor. */
  public record Route(Event event, ActorRef<ConversationActor.Reply> replyTo) {}

  static Behavior<Route> guardian() {
    return Behaviors.setup(
        ctx -> {
          Map<String, ActorRef<ConversationActor.Command>> children = new HashMap<>();
          return Behaviors.receive(Route.class)
              .onMessage(
                  Route.class,
                  msg -> {
                    ActorRef<ConversationActor.Command> child =
                        children.computeIfAbsent(
                            msg.event().conversationId(),
                            cid -> ctx.spawn(ConversationActor.create(cid), "conv-" + cid));
                    child.tell(new ConversationActor.ProcessTurn(msg.event(), msg.replyTo()));
                    return Behaviors.same();
                  })
              .build();
        });
  }

  public static void main(String[] args) throws Exception {
    ActorSystem<Route> system = ActorSystem.create(guardian(), "agentic-pekko");
    Duration timeout = Duration.ofSeconds(5);
    try {
      List<Event> turns =
          List.of(
              new Event("c1", "alice", "what card types do you offer?"),
              new Event("c2", "bob", "what is my balance?"),
              new Event("c1", "alice", "tell me about crypto cash-back"),
              new Event("c3", "carol", "hello there"));
      for (Event e : turns) {
        CompletionStage<ConversationActor.Reply> cs =
            AskPattern.ask(system, replyTo -> new Route(e, replyTo), timeout, system.scheduler());
        ConversationActor.Reply r = cs.toCompletableFuture().get();
        System.out.printf("[%s] path=%s ok=%s reply=%s%n", e.conversationId(), r.path(), r.ok(), r.reply());
      }
    } finally {
      system.terminate();
    }
  }
}
