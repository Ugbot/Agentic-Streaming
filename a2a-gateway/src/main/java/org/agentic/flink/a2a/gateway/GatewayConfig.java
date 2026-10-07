package org.agentic.flink.a2a.gateway;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;

/**
 * Resolved gateway configuration, sourced from {@link AgenticFlinkConfig} (explicit properties >
 * {@code AGENTIC_FLINK_*} env vars > system properties > defaults).
 *
 * <p>Covers the published Agent Card identity, the {@code a2a.bridge.*} transport, and the request
 * timeout. Quarkus HTTP ports are configured separately in {@code application.properties}.
 *
 * <p>JSON-RPC (with SSE for {@code message/stream}) is the only inbound transport this gateway
 * serves. Construction rejects the legacy {@code a2a.gateway.grpc.url} and {@code a2a.gateway.rest.url}
 * keys ({@link #UNSUPPORTED_TRANSPORT_KEYS}) instead of advertising an endpoint nothing listens on.
 */
@ApplicationScoped
public class GatewayConfig {

  /** Config keys for transports this module does not implement; setting one is a startup error. */
  public static final List<String> UNSUPPORTED_TRANSPORT_KEYS =
      List.of("a2a.gateway.grpc.url", "a2a.gateway.rest.url");

  /** One Agent Card skill, parsed from {@code a2a.gateway.agent.skills}. */
  public record Skill(String id, String name, String description) {}

  private static final Skill GENERIC_SKILL = new Skill(
      "agent", "Agent", "Send a message to the agent and receive its reply.");

  private final AgenticFlinkConfig config;

  public GatewayConfig() {
    this(AgenticFlinkConfig.fromEnvironment());
  }

  public GatewayConfig(AgenticFlinkConfig config) {
    this.config = config;
    for (String key : UNSUPPORTED_TRANSPORT_KEYS) {
      String v = config.get(key, "");
      if (!v.isBlank()) {
        throw new IllegalStateException(key + " is set to '" + v + "' but this gateway serves only JSON-RPC"
            + " (with SSE); it has no gRPC or REST binding to advertise. Unset it.");
      }
    }
  }

  public AgenticFlinkConfig raw() {
    return config;
  }

  public String agentId() {
    return config.get("a2a.gateway.agent.id", "agentic-flink");
  }

  public String agentName() {
    return config.get("a2a.gateway.agent.name", "Agentic Flink Agent");
  }

  public String agentDescription() {
    return config.get(
        "a2a.gateway.agent.description",
        "A Flink-hosted agent exposed over the A2A protocol.");
  }

  public String agentVersion() {
    return config.get("a2a.gateway.agent.version", "1.0.0");
  }

  public String publicUrl() {
    return config.get(
        ConfigKeys.A2A_GATEWAY_PUBLIC_URL, "http://localhost:9999");
  }

  public String protocolVersion() {
    return config.get(ConfigKeys.A2A_PROTOCOL_VERSION, ConfigKeys.DEFAULT_A2A_PROTOCOL_VERSION);
  }

  public long requestTimeoutMs() {
    return config.getInt("a2a.gateway.request.timeout.ms", 60_000);
  }

  /** Comma-separated skill descriptors {@code id:name:description}; empty -> one generic skill. */
  public String skillsSpec() {
    return config.get("a2a.gateway.agent.skills", "");
  }

  /**
   * The skills the Agent Card lists, parsed from {@link #skillsSpec()}. A descriptor with fewer than
   * three colon-separated fields is rejected so a typo cannot silently publish an empty skill.
   */
  public List<Skill> skills() {
    String spec = skillsSpec();
    if (spec == null || spec.isBlank()) {
      return List.of(GENERIC_SKILL);
    }
    List<Skill> out = new ArrayList<>();
    for (String item : spec.split(",")) {
      if (item.isBlank()) {
        continue;
      }
      String[] parts = item.trim().split(":", 3);
      if (parts.length < 3 || parts[0].isBlank() || parts[1].isBlank()) {
        throw new IllegalStateException("a2a.gateway.agent.skills entry '" + item.trim()
            + "' must be id:name:description");
      }
      out.add(new Skill(parts[0].trim(), parts[1].trim(), parts[2].trim()));
    }
    return out.isEmpty() ? List.of(GENERIC_SKILL) : List.copyOf(out);
  }

  /** Task-store backend for gateway-side lifecycle persistence: {@code memory|redis|postgres}. */
  public String taskStoreBackend() {
    return config.get(ConfigKeys.A2A_TASK_STORE, ConfigKeys.DEFAULT_A2A_TASK_STORE);
  }

  /**
   * Connection config passed to the task store's {@code initialize} — the relevant Redis/Postgres
   * keys from the environment. {@code memory} ignores it.
   */
  public java.util.Map<String, String> taskStoreConfig() {
    java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
    putIfPresent(m, ConfigKeys.REDIS_HOST);
    putIfPresent(m, ConfigKeys.REDIS_PORT);
    putIfPresent(m, ConfigKeys.REDIS_PASSWORD);
    putIfPresent(m, ConfigKeys.POSTGRES_URL);
    putIfPresent(m, ConfigKeys.POSTGRES_USER);
    putIfPresent(m, ConfigKeys.POSTGRES_PASSWORD);
    return m;
  }

  private void putIfPresent(java.util.Map<String, String> m, String key) {
    String v = config.get(key);
    if (v != null && !v.isEmpty()) {
      m.put(key, v);
    }
  }

  /** Whether the Agent Card advertises streaming (message/stream SSE). */
  public boolean streamingEnabled() {
    return Boolean.parseBoolean(config.get("a2a.gateway.streaming.enabled", "false"));
  }

  /** Whether the Agent Card advertises push notifications. */
  public boolean pushEnabled() {
    return Boolean.parseBoolean(config.get("a2a.gateway.push.enabled", "false"));
  }

  /**
   * Bearer tokens accepted by the gateway: {@code a2a.auth.tokens} ({@code AGENTIC_FLINK_A2A_AUTH_TOKENS})
   * as {@code subject=token,...} or a bare token, falling back to the {@code AGENTIC_A2A_TOKEN}
   * environment variable. Empty means unauthenticated access is refused unless {@link #authDevMode()}.
   */
  public String authTokens() {
    String v = config.get("a2a.auth.tokens", "");
    if (v != null && !v.isBlank()) {
      return v;
    }
    String env = System.getenv("AGENTIC_A2A_TOKEN");
    return env == null ? "" : env;
  }

  /** Explicit development override: admit unauthenticated callers when no token is configured. */
  public boolean authDevMode() {
    return Boolean.parseBoolean(config.get("a2a.auth.dev.mode", "false"));
  }

  /**
   * Comma-separated host allowlist for push notification webhooks ({@code a2a.push.allowed.hosts}).
   * Empty means any public host; private, loopback, link-local and metadata addresses are always denied.
   */
  public String pushAllowedHosts() {
    return config.get("a2a.push.allowed.hosts", "");
  }
}
