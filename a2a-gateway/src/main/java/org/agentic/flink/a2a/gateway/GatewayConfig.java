package org.agentic.flink.a2a.gateway;

import jakarta.enterprise.context.ApplicationScoped;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;

/**
 * Resolved gateway configuration, sourced from {@link AgenticFlinkConfig} (explicit properties >
 * {@code AGENTIC_FLINK_*} env vars > system properties > defaults).
 *
 * <p>Covers the published Agent Card identity, the {@code a2a.bridge.*} transport, and the request
 * timeout. Quarkus HTTP ports are configured separately in {@code application.properties}.
 */
@ApplicationScoped
public class GatewayConfig {

  private final AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();

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

  /** Public gRPC endpoint to advertise on the Agent Card; empty = not advertised. */
  public String grpcUrl() {
    return config.get("a2a.gateway.grpc.url", "");
  }

  /** Public HTTP+JSON (REST) endpoint to advertise on the Agent Card; empty = not advertised. */
  public String restUrl() {
    return config.get("a2a.gateway.rest.url", "");
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
}
