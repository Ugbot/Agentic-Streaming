package org.jagentic.pekko;

import java.time.Duration;
import java.util.List;

import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;

/** Single-node demo: boot the Pekko system over the banking graph and run a few turns through
 * the event-sourced conversation entities. Run with {@code mvn -f agentic-pekko/pom.xml exec:java}. */
public final class Main {

  private Main() {}

  public static void main(String[] args) {
    AgentDeps deps = AgentDeps.banking();
    try (PekkoSystem sys = new PekkoSystem(deps)) {
      PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(10));
      List<Event> turns = List.of(
          new Event("c1", "alice", "what card types do you offer?"),
          new Event("c2", "bob", "what is my balance?"),
          new Event("c1", "alice", "tell me about crypto cash-back"),
          new Event("c3", "carol", "hello there"));
      for (Event e : turns) {
        TurnResult r = rt.submit(e);
        System.out.printf("[%s] path=%-8s ok=%s reply=%s%n", e.conversationId(), r.path, r.ok, r.reply);
      }
    }
  }
}
