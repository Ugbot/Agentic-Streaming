package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The A2A transport bindings a peer can speak. The protocol defines three functionally-equivalent
 * bindings advertised in the {@link A2AAgentCard} via {@code preferredTransport} /
 * {@code additionalInterfaces}.
 */
public enum A2ATransport {
  /** JSON-RPC 2.0 over HTTP (the most widely deployed binding). */
  JSONRPC("JSONRPC"),
  /** gRPC using the A2AService proto. */
  GRPC("GRPC"),
  /** Google-API-style HTTP + JSON with custom verbs. */
  HTTP_JSON("HTTP+JSON");

  private final String wire;

  A2ATransport(String wire) {
    this.wire = wire;
  }

  @JsonValue
  public String wire() {
    return wire;
  }

  @JsonCreator
  public static A2ATransport fromWire(String wire) {
    if (wire == null) {
      return JSONRPC;
    }
    String normalized = wire.trim().toUpperCase().replace("-", "_").replace("+", "_");
    switch (normalized) {
      case "GRPC":
        return GRPC;
      case "HTTP_JSON":
      case "HTTP":
      case "REST":
        return HTTP_JSON;
      case "JSONRPC":
      case "JSON_RPC":
      default:
        return JSONRPC;
    }
  }
}
