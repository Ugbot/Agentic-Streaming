package org.jagentic.pekko;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.Scheduler;
import org.apache.pekko.actor.typed.Terminated;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

import org.jagentic.core.Event;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.entity.ConversationEntity.GetState;
import org.jagentic.pekko.entity.ConversationEntity.ProcessTurn;
import org.jagentic.pekko.entity.ConversationEntity.StateSnapshot;
import org.jagentic.pekko.entity.ConversationEntity.TurnReply;
import org.jagentic.pekko.runtime.AgentDeps;

/**
 * Distinctive Pekko showcase: <b>durability / recovery across restart</b>. We run a couple of turns
 * on a conversation, then <b>passivate</b> the event-sourced entity (drop it from memory). The next
 * read recreates the entity, which <b>rehydrates its transcript from the event journal</b> — the
 * persisted {@code TurnCommitted} events are replayed, <i>without</i> re-invoking the LLM/tool
 * pipeline. The message count before passivation equals the count after recovery.
 *
 * <p>This runs on the default in-memory journal (events survive within the {@code ActorSystem}
 * lifetime). For recovery across a real JVM/process restart, run with a durable journal:
 * {@code -Dconfig.resource=application-cluster-jdbc.conf} (+ {@code AGENTIC_PG_URL}) or the
 * Cassandra profile — same entity, same code, the events simply outlive the process.
 *
 * <pre>mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.RecoveryDemo</pre>
 */
public final class RecoveryDemo {

  private RecoveryDemo() {}

  // ---- a tiny guardian that owns one entity and can passivate + respawn it (same persistenceId) ----

  /** Guardian protocol (local only — never serialized under provider=local). */
  public interface G {}

  /** Forward a command to the live entity. */
  public record Forward(ConversationEntity.Command cmd) implements G {}

  /** Passivate: stop the entity; once it has terminated, respawn it (recovers from the journal). */
  public record Restart(ActorRef<Done> ack) implements G {}

  static Behavior<G> guardian(String conversationId, AgentDeps deps) {
    return Behaviors.setup(ctx -> new Guardian(ctx, conversationId, deps).behavior());
  }

  /** Holds the current entity ref + a pending passivation ack; respawns on Terminated. */
  static final class Guardian {
    private final ActorContext<G> ctx;
    private final String conversationId;
    private final AgentDeps deps;
    private ActorRef<ConversationEntity.Command> entity;
    private int incarnation = 0;
    private ActorRef<Done> pendingAck;

    Guardian(ActorContext<G> ctx, String conversationId, AgentDeps deps) {
      this.ctx = ctx;
      this.conversationId = conversationId;
      this.deps = deps;
      spawnEntity();
    }

    private void spawnEntity() {
      // Same conversationId → same PersistenceId ("Conversation|<id>") → recovery replays its events.
      entity = ctx.spawn(ConversationEntity.create(conversationId, deps),
          "conv-" + conversationId + "-" + (incarnation++));
      ctx.watch(entity);
    }

    Behavior<G> behavior() {
      return Behaviors.receive(G.class)
          .onMessage(Forward.class, f -> {
            entity.tell(f.cmd());
            return Behaviors.same();
          })
          .onMessage(Restart.class, r -> {
            pendingAck = r.ack();
            ctx.stop(entity); // triggers Terminated below once fully stopped
            return Behaviors.same();
          })
          .onSignal(Terminated.class, t -> {
            spawnEntity(); // exactly one live entity per persistenceId at a time
            if (pendingAck != null) {
              pendingAck.tell(Done.getInstance());
              pendingAck = null;
            }
            return Behaviors.same();
          })
          .build();
    }
  }

  // ---- the demo ----

  public static void main(String[] args) throws Exception {
    String cid = "c1";
    ActorSystem<G> system = ActorSystem.create(guardian(cid, AgentDeps.banking()), "recovery-demo");
    Duration timeout = Duration.ofSeconds(10);
    Scheduler scheduler = system.scheduler();
    try {
      System.out.println("— running two turns —");
      System.out.println("  " + turn(system, timeout, scheduler, "t1", cid, "what card types do you offer?"));
      System.out.println("  " + turn(system, timeout, scheduler, "t2", cid, "tell me about crypto cash-back"));

      int before = snapshot(system, timeout, scheduler).messageCount();
      System.out.println("before passivation: messages=" + before);

      System.out.println("— passivating the entity (dropped from memory) —");
      AskPattern.<G, Done>ask(system, Restart::new, timeout, scheduler).toCompletableFuture().get();

      System.out.println("— next read recreates the entity, rehydrating from the event journal —");
      int after = snapshot(system, timeout, scheduler).messageCount();
      System.out.println("after recovery:     messages=" + after
          + (before == after ? "  ✓ transcript survived restart (no LLM re-run)" : "  ✗ MISMATCH"));
    } finally {
      system.terminate();
    }
  }

  private static String turn(ActorSystem<G> system, Duration timeout, Scheduler scheduler,
                             String turnId, String cid, String text) throws Exception {
    CompletionStage<TurnReply> cs = AskPattern.ask(system,
        (ActorRef<TurnReply> replyTo) -> new Forward(new ProcessTurn(turnId, new Event(cid, "alice", text), replyTo)),
        timeout, scheduler);
    TurnReply r = cs.toCompletableFuture().get();
    return "[" + r.path() + "] " + r.reply();
  }

  private static StateSnapshot snapshot(ActorSystem<G> system, Duration timeout, Scheduler scheduler)
      throws Exception {
    return AskPattern.<G, StateSnapshot>ask(system,
            replyTo -> new Forward(new GetState(replyTo)), timeout, scheduler)
        .toCompletableFuture().get();
  }
}
