package org.jagentic.ports.pulsar;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pulsar.functions.api.Context;

/**
 * Runs {@link BankingFunction} with <b>no Pulsar cluster</b>: a single {@link
 * InMemoryContext} stands in for the broker + BookKeeper state store, exactly as
 * Pekko's {@code LocalDemo} stands in for a cluster. State persists across turns in
 * one map, so {@code c1}'s two turns hit the same persisted conversation envelope —
 * proving the Pulsar state store carries C1.
 *
 * <p>In production you instead deploy {@code BankingFunction} to a Pulsar cluster:
 * {@code pulsar-admin functions create --jar agentic-pulsar.jar --classname
 * org.jagentic.ports.pulsar.BankingFunction --inputs banking-requests --output
 * banking-responses --subscription-type Key_Shared}. The state then lives in
 * BookKeeper and Key_Shared keying by {@code conversationId} gives single-writer
 * ordering across instances.
 */
public final class LocalDemo {

  private LocalDemo() {}

  public static void main(String[] args) {
    BankingFunction fn = new BankingFunction();
    // One state map for all conversation keys, as one deployed function instance
    // namespaces state across the keys it owns.
    Map<String, byte[]> state = new ConcurrentHashMap<>();

    List<String[]> turns = Arrays.asList(
        new String[] {"c1", "what card types do you offer?"},
        new String[] {"c2", "what is my balance?"},
        new String[] {"c1", "tell me about crypto cash-back"},
        new String[] {"c3", "where is the nearest branch?"});

    System.out.println("=== Agentic-Flink :: Banking RoutedGraph as a Pulsar Function ===");
    System.out.println("state: in-memory stand-in for Pulsar's BookKeeper state store\n");

    Map<String, Integer> turnCounts = new HashMap<>();
    for (String[] t : turns) {
      String cid = t[0];
      String text = t[1];
      Context ctx = InMemoryContext.create(InMemoryContext.record(cid, text, "demo"), state);
      String reply = fn.process(text, ctx);
      turnCounts.merge(cid, 1, Integer::sum);
      System.out.printf("[%s] turn=%d reply=%s%n", cid, turnCounts.get(cid), reply);
    }

    // Prove C1: the persisted envelope for c1 holds both of its turns (4 messages:
    // user+assistant x2), recovered straight from the state store.
    Context probe = InMemoryContext.create(InMemoryContext.record("c1", "", "demo"), state);
    PulsarStateConversationStore store =
        new PulsarStateConversationStore(BankingFunction.stateBytes(probe));
    System.out.printf("%nc1 persisted message count = %d (state survives across turns)%n",
        store.messageCount("c1"));
  }
}
