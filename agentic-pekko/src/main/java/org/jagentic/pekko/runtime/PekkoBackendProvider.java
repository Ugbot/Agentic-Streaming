package org.jagentic.pekko.runtime;

import java.time.Duration;

import org.jagentic.core.ConversationStore;
import org.jagentic.core.Runtime;
import org.jagentic.core.pipeline.BackendProvider;
import org.jagentic.core.pipeline.GraphBuilder;

/** Makes {@code backend: pekko} resolvable from a {@code pipeline.yaml}: builds the agent brain
 * from the compiled {@link GraphBuilder.Built}, boots a local Pekko system, and returns a
 * {@link PekkoRuntime}. The Pekko entity is event-sourced, so it owns its own durable state — the
 * loader's {@code conversationStore} is intentionally not used here. Registered via
 * {@code META-INF/services/org.jagentic.core.pipeline.BackendProvider}. */
public final class PekkoBackendProvider implements BackendProvider {

  @Override
  public String name() {
    return "pekko";
  }

  @Override
  public Runtime create(GraphBuilder.Built built, ConversationStore conversationStore) {
    AgentDeps deps = new AgentDeps(built.graph(), built.tools(), built.retriever());
    PekkoSystem system = new PekkoSystem(deps);
    return new PekkoRuntime(system.system(), Duration.ofSeconds(30), true);
  }
}
