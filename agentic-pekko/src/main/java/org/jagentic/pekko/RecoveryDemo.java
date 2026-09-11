package org.jagentic.pekko;

import java.time.Duration;

import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.pekko.entity.ConversationEntity.StateSnapshot;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;

/**
 * Distinctive Pekko showcase: <b>durability / recovery across restart</b>. We run a couple of turns
 * on a conversation, then <b>passivate</b> the event-sourced entity (drop it from memory). The next
 * command recreates the entity, which rebuilds its state by folding the journal — the persisted
 * spec log events are replayed, <i>without</i> re-invoking the LLM/tool pipeline. The turn count and
 * transcript before passivation equal those after recovery, and a redelivered turn is answered from
 * the journal as a {@code duplicate}.
 *
 * <p>This runs on the {@code MEMORY} profile (events survive within the {@code ActorSystem}
 * lifetime). For recovery across a real JVM/process restart, select a durable journal with
 * {@code AGENTIC_PEKKO_DURABILITY=postgres} (+ {@code AGENTIC_PG_URL}) or {@code cassandra} — same
 * entity, same code, the events simply outlive the process.
 *
 * <pre>mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.RecoveryDemo</pre>
 */
public final class RecoveryDemo {

  private RecoveryDemo() {}

  public static void main(String[] args) {
    String cid = "c1";
    try (PekkoSystem sys = new PekkoSystem(AgentDeps.banking());
         PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(10))) {
      System.out.println("— running two turns —");
      System.out.println("  " + show(rt.submit(Event.turn(cid, "t1", "alice", "what card types do you offer?"))));
      System.out.println("  " + show(rt.submit(Event.turn(cid, "t2", "alice", "tell me about crypto cash-back"))));

      StateSnapshot before = rt.state(cid);
      System.out.println("before passivation: turns=" + before.turnCount() + " messages=" + before.messageCount()
          + " journal=" + before.events().size());

      System.out.println("— passivating the entity (dropped from memory) —");
      rt.passivate(cid);

      System.out.println("— next command recreates the entity, folding the event journal —");
      StateSnapshot after = rt.state(cid);
      boolean same = before.turnCount() == after.turnCount() && before.events().equals(after.events());
      System.out.println("after recovery:     turns=" + after.turnCount() + " messages=" + after.messageCount()
          + " journal=" + after.events().size()
          + (same ? "  ✓ state survived restart (no LLM re-run)" : "  ✗ MISMATCH"));

      TurnResult dup = rt.submit(Event.turn(cid, "t1", "alice", "what card types do you offer?"));
      System.out.println("redelivered t1 → status=" + dup.status.wire() + " events appended=" + dup.events.size());
    }
  }

  private static String show(TurnResult r) {
    return "[" + r.path + "] " + r.reply;
  }
}
