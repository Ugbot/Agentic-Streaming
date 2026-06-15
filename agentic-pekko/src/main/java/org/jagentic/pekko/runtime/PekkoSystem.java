package org.jagentic.pekko.runtime;

import org.apache.pekko.actor.typed.ActorSystem;

/** Boots the Pekko {@link ActorSystem} whose guardian routes turns to per-conversation entities.
 * AutoCloseable so demos/tests tear it down. */
public final class PekkoSystem implements AutoCloseable {

  public static final String SYSTEM_NAME = "AgenticPekko";

  private final ActorSystem<ConversationManager.Command> system;

  public PekkoSystem(AgentDeps deps) {
    this.system = ActorSystem.create(ConversationManager.create(deps), SYSTEM_NAME);
  }

  public ActorSystem<ConversationManager.Command> system() {
    return system;
  }

  @Override
  public void close() {
    system.terminate();
  }
}
