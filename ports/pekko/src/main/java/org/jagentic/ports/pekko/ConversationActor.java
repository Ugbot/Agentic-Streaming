package org.jagentic.ports.pekko;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.jagentic.core.AgentContext;
import org.jagentic.core.Banking;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.TurnResult;

/**
 * The per-conversation agent as an Apache Pekko typed actor.
 *
 * <p>A Pekko actor is the ideal home for the agent essence: it has a mailbox
 * (messages processed one at a time = single-writer, ordered — C2) and private
 * fields (its state — C1). One actor instance per {@code conversationId} (via
 * {@link BankingSharding Cluster Sharding}) is exactly Flink's keyed operator, on
 * the actor model. The ask pattern makes turns async (C4); Pekko Persistence
 * (event sourcing) makes the state durable (C3).
 *
 * <p>The agent logic itself is the Flink-free {@code jagentic-core} —
 * {@link Banking#buildGraph()} — reused verbatim; only the actor seam is Pekko.
 */
public final class ConversationActor {

  /** Messages this entity accepts. Marked serializable for remoting/sharding
   * (production binds pekko-serialization-jackson via application.conf). */
  public sealed interface Command extends java.io.Serializable permits ProcessTurn {}

  /** Run one turn for this conversation and reply with the result. */
  public static final class ProcessTurn implements Command {
    private static final long serialVersionUID = 1L;
    public final Event event;
    public final ActorRef<Reply> replyTo;

    public ProcessTurn(Event event, ActorRef<Reply> replyTo) {
      this.event = event;
      this.replyTo = replyTo;
    }
  }

  /** The turn result sent back to the asker. */
  public record Reply(String reply, String path, boolean ok) implements java.io.Serializable {}

  private ConversationActor() {}

  /** Behavior factory — one per {@code conversationId}, over the default banking
   * essence. */
  public static Behavior<Command> create(String conversationId) {
    return create(conversationId, Banking.buildGraph(), Banking.defaultTools(), Banking.retriever());
  }

  /** Injectable behavior factory — run any graph/tools/retriever built from the public
   * core abstractions (e.g. an extended graph) on the Pekko actor seam. Its captured
   * stores ARE the keyed state for that conversation. */
  public static Behavior<Command> create(String conversationId, RoutedGraph graph,
                                          ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {
    return Behaviors.setup(
        ctx -> {
          // Per-conversation state lives in the actor (single-writer). For durability
          // across restarts, replace this with an EventSourcedBehavior (Pekko
          // Persistence) or write through to a Redis/Fluss-backed ConversationStore.
          final ConversationStore store = new ConversationStore.InMemory();
          final KeyedStateStore state = new KeyedStateStore.InMemory();

          return Behaviors.receive(Command.class)
              .onMessage(
                  ProcessTurn.class,
                  msg -> {
                    AgentContext actx =
                        new AgentContext(conversationId, msg.event.userId(), store, state, tools, retriever);
                    TurnResult r = graph.handle(msg.event, actx);
                    msg.replyTo.tell(new Reply(r.reply, r.path, r.ok));
                    return Behaviors.same();
                  })
              .build();
        });
  }
}
