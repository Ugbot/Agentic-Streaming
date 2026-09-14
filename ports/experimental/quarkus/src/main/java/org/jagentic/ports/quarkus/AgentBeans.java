package org.jagentic.ports.quarkus;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import org.jagentic.core.ConversationStore;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.Banking;
import org.jagentic.core.ToolRegistry;

/**
 * CDI producers that expose the engine-agnostic {@code jagentic-core} singletons as
 * injectable, {@code @ApplicationScoped} beans. These are the portable C1 (state) and
 * C7 (tools) SPIs from the Quarkus design doc — the in-memory implementations stand in
 * for a Redis/Fluss backend, swappable behind the same interface without touching the
 * agent beans. Application-scoped so a single conversation store is shared across the
 * REST edge ({@link AgentResource}) and the streaming edge ({@link BankingStream}).
 */
@ApplicationScoped
public class AgentBeans {

  @Produces
  @Singleton
  public ConversationStore conversationStore() {
    return new ConversationStore.InMemory();
  }

  @Produces
  @Singleton
  public KeyedStateStore keyedStateStore() {
    return new KeyedStateStore.InMemory();
  }

  @Produces
  @Singleton
  public ToolRegistry toolRegistry() {
    return Banking.defaultTools();
  }

  /** Hot-tier retriever seeded with the banking KB (cold tier = null). */
  @Produces
  @Singleton
  public Retrieval.TwoTierRetriever retriever() {
    return Banking.retriever();
  }

  /** The canonical {@code router -> path -> verifier} banking topology. */
  @Produces
  @Singleton
  public RoutedGraph routedGraph() {
    return Banking.buildGraph();
  }
}
