package org.jagentic.ports.quarkus;

/** Wire DTOs for both the REST edge and the Kafka channels (Jackson-serializable records). */
public final class AgentMessages {
  private AgentMessages() {}

  /** Inbound turn. {@code conversationId} is the partition / state key. */
  public record AgentRequest(String conversationId, String userId, String text) {}

  /** Outbound turn result. */
  public record AgentReply(String conversationId, String path, boolean ok, String reply) {}
}
