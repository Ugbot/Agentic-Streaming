package org.agentic.flink.a2a;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Serializable configuration for a remote A2A agent ("peer") this job can call as a workflow step.
 *
 * <p>Directly analogous to {@link org.agentic.flink.tools.mcp.McpServerSpec}: an immutable,
 * Java-serializable description that ships in the Flink job graph, from which the live {@link
 * A2AClient} is constructed on the task side (in {@code RichFunction.open()}). A peer is reached
 * one of two ways:
 *
 * <ul>
 *   <li>{@link #card(String, String)} — point at the peer's {@code /.well-known/agent-card.json};
 *       the endpoint, transport, and skills are discovered at runtime.
 *   <li>{@link #endpoint(String, String, A2ATransport)} — pin a known endpoint + transport directly
 *       (skips card discovery).
 * </ul>
 *
 * <p>{@link #skillId()} optionally targets a specific advertised skill; {@link #auth()} supplies
 * credentials; {@link #streaming()} requests {@code message/stream} (SSE) where the peer supports
 * it, otherwise the client falls back to {@code message/send} + {@code tasks/get} polling.
 */
public final class RemoteAgentSpec implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final String agentCardUrl; // for card discovery
  private final String endpointUrl; // for pinned endpoint
  private final A2ATransport transport;
  private final String skillId;
  private final AuthSpec auth;
  private final boolean streaming;
  private final long requestTimeoutMs;
  private final long pollIntervalMs;
  private final String description;
  private final int maxRetries;
  private final long retryBaseBackoffMs;
  private final long retryMaxBackoffMs;
  private final int circuitBreakerThreshold;
  private final long circuitBreakerOpenMs;

  private RemoteAgentSpec(Builder b) {
    this.name = Objects.requireNonNull(b.name, "name");
    this.agentCardUrl = b.agentCardUrl;
    this.endpointUrl = b.endpointUrl;
    this.transport = b.transport == null ? A2ATransport.JSONRPC : b.transport;
    this.skillId = b.skillId;
    this.auth = b.auth == null ? AuthSpec.none() : b.auth;
    this.streaming = b.streaming;
    this.requestTimeoutMs = b.requestTimeoutMs;
    this.pollIntervalMs = b.pollIntervalMs;
    this.description = b.description;
    this.maxRetries = b.maxRetries;
    this.retryBaseBackoffMs = b.retryBaseBackoffMs;
    this.retryMaxBackoffMs = b.retryMaxBackoffMs;
    this.circuitBreakerThreshold = b.circuitBreakerThreshold;
    this.circuitBreakerOpenMs = b.circuitBreakerOpenMs;
    if (agentCardUrl == null && endpointUrl == null) {
      throw new IllegalArgumentException(
          "RemoteAgentSpec requires either an agentCardUrl (discovery) or an endpointUrl (pinned)");
    }
  }

  /** Discover a peer from its Agent Card. Example: {@code card("planner", "https://x/.well-known/agent-card.json")}. */
  public static RemoteAgentSpec card(String name, String agentCardUrl) {
    return builder().withName(name).withAgentCardUrl(agentCardUrl).build();
  }

  /** Pin a peer's endpoint + transport directly, skipping card discovery. */
  public static RemoteAgentSpec endpoint(String name, String endpointUrl, A2ATransport transport) {
    return builder().withName(name).withEndpointUrl(endpointUrl).withTransport(transport).build();
  }

  public String name() {
    return name;
  }

  public String agentCardUrl() {
    return agentCardUrl;
  }

  public String endpointUrl() {
    return endpointUrl;
  }

  public A2ATransport transport() {
    return transport;
  }

  public String skillId() {
    return skillId;
  }

  public AuthSpec auth() {
    return auth;
  }

  public boolean streaming() {
    return streaming;
  }

  public long requestTimeoutMs() {
    return requestTimeoutMs;
  }

  public long pollIntervalMs() {
    return pollIntervalMs;
  }

  public String description() {
    return description;
  }

  /** Max retry attempts for a transient remote failure (in addition to the initial try). Default 2. */
  public int maxRetries() {
    return maxRetries;
  }

  /** Base delay for exponential backoff between retries, in ms. Default 100. */
  public long retryBaseBackoffMs() {
    return retryBaseBackoffMs;
  }

  /** Cap on the exponential backoff delay, in ms. Default 2000. */
  public long retryMaxBackoffMs() {
    return retryMaxBackoffMs;
  }

  /** Consecutive failures (per peer) that trip the circuit breaker open. Default 5. */
  public int circuitBreakerThreshold() {
    return circuitBreakerThreshold;
  }

  /** How long the breaker stays open before allowing a half-open trial call, in ms. Default 30s. */
  public long circuitBreakerOpenMs() {
    return circuitBreakerOpenMs;
  }

  /** The synthetic tool id this peer is registered under: {@code "a2a:" + name}. */
  public String toolId() {
    return "a2a:" + name;
  }

  /** True when this spec discovers its endpoint via an Agent Card rather than a pinned URL. */
  public boolean usesCardDiscovery() {
    return endpointUrl == null;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private String agentCardUrl;
    private String endpointUrl;
    private A2ATransport transport;
    private String skillId;
    private AuthSpec auth;
    private boolean streaming = false;
    private long requestTimeoutMs = Duration.ofSeconds(60).toMillis();
    private long pollIntervalMs = Duration.ofMillis(500).toMillis();
    private String description;
    private int maxRetries = 2;
    private long retryBaseBackoffMs = 100;
    private long retryMaxBackoffMs = 2000;
    private int circuitBreakerThreshold = 5;
    private long circuitBreakerOpenMs = Duration.ofSeconds(30).toMillis();

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withAgentCardUrl(String agentCardUrl) {
      this.agentCardUrl = agentCardUrl;
      return this;
    }

    public Builder withEndpointUrl(String endpointUrl) {
      this.endpointUrl = endpointUrl;
      return this;
    }

    public Builder withTransport(A2ATransport transport) {
      this.transport = transport;
      return this;
    }

    public Builder withSkillId(String skillId) {
      this.skillId = skillId;
      return this;
    }

    public Builder withAuth(AuthSpec auth) {
      this.auth = auth;
      return this;
    }

    public Builder withStreaming(boolean streaming) {
      this.streaming = streaming;
      return this;
    }

    public Builder withRequestTimeout(Duration timeout) {
      this.requestTimeoutMs = timeout.toMillis();
      return this;
    }

    public Builder withPollInterval(Duration interval) {
      this.pollIntervalMs = interval.toMillis();
      return this;
    }

    public Builder withDescription(String description) {
      this.description = description;
      return this;
    }

    /** Max retry attempts on transient failure (0 disables retry). */
    public Builder withMaxRetries(int maxRetries) {
      this.maxRetries = Math.max(0, maxRetries);
      return this;
    }

    /** Exponential-backoff base and cap between retries. */
    public Builder withRetryBackoff(Duration base, Duration max) {
      this.retryBaseBackoffMs = Math.max(0, base.toMillis());
      this.retryMaxBackoffMs = Math.max(this.retryBaseBackoffMs, max.toMillis());
      return this;
    }

    /** Consecutive-failure count that trips the breaker open (≤0 disables the breaker). */
    public Builder withCircuitBreakerThreshold(int threshold) {
      this.circuitBreakerThreshold = threshold;
      return this;
    }

    /** How long the breaker stays open before a half-open trial. */
    public Builder withCircuitBreakerOpen(Duration open) {
      this.circuitBreakerOpenMs = Math.max(0, open.toMillis());
      return this;
    }

    public RemoteAgentSpec build() {
      return new RemoteAgentSpec(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RemoteAgentSpec)) {
      return false;
    }
    RemoteAgentSpec that = (RemoteAgentSpec) o;
    return streaming == that.streaming
        && requestTimeoutMs == that.requestTimeoutMs
        && pollIntervalMs == that.pollIntervalMs
        && Objects.equals(name, that.name)
        && Objects.equals(agentCardUrl, that.agentCardUrl)
        && Objects.equals(endpointUrl, that.endpointUrl)
        && transport == that.transport
        && Objects.equals(skillId, that.skillId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, agentCardUrl, endpointUrl, transport, skillId, streaming, requestTimeoutMs, pollIntervalMs);
  }

  @Override
  public String toString() {
    return "RemoteAgentSpec{name=" + name + ", target="
        + (endpointUrl != null ? endpointUrl + " (" + transport + ")" : agentCardUrl + " (card)")
        + (skillId != null ? ", skill=" + skillId : "") + '}';
  }
}
