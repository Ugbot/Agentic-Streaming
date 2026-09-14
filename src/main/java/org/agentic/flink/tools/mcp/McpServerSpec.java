package org.agentic.flink.tools.mcp;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Serializable configuration for a Model Context Protocol (MCP) server.
 *
 * <p>Two transports are supported:
 *
 * <ul>
 *   <li>{@link Transport#STDIO} — spawn a subprocess and exchange newline-delimited JSON-RPC
 *       over its stdin/stdout. The dominant local transport.
 *   <li>{@link Transport#HTTP} — POST JSON-RPC payloads to a URL. Used by hosted MCP servers.
 * </ul>
 *
 * <p>Specs are immutable and Java-serializable so they can ship in the Flink job graph; the
 * actual {@link McpClient} is constructed on the task side from this spec.
 */
public final class McpServerSpec implements Serializable {
  private static final long serialVersionUID = 2L;

  public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

  /** Underlying wire transport. */
  public enum Transport {
    STDIO,
    HTTP
  }

  private final String name;
  private final Transport transport;
  private final List<String> command; // for STDIO
  private final Map<String, String> env; // for STDIO
  private final String url; // for HTTP
  private final Map<String, String> headers; // for HTTP
  private final Duration connectTimeout; // for HTTP
  private final Duration requestTimeout; // for HTTP

  private McpServerSpec(Builder b) {
    this.name = Objects.requireNonNull(b.name, "name");
    this.transport = Objects.requireNonNull(b.transport, "transport");
    this.command = b.command == null ? Collections.emptyList() : List.copyOf(b.command);
    this.env = b.env == null ? Collections.emptyMap() : Map.copyOf(b.env);
    this.url = b.url;
    this.headers = b.headers == null ? Collections.emptyMap() : Map.copyOf(b.headers);
    this.connectTimeout = positive(b.connectTimeout, DEFAULT_CONNECT_TIMEOUT, "connectTimeout");
    this.requestTimeout = positive(b.requestTimeout, DEFAULT_REQUEST_TIMEOUT, "requestTimeout");

    if (transport == Transport.STDIO && command.isEmpty()) {
      throw new IllegalArgumentException("STDIO transport requires non-empty command");
    }
    if (transport == Transport.HTTP && (url == null || url.isEmpty())) {
      throw new IllegalArgumentException("HTTP transport requires url");
    }
  }

  public String getName() {
    return name;
  }

  public Transport getTransport() {
    return transport;
  }

  public List<String> getCommand() {
    return command;
  }

  public Map<String, String> getEnv() {
    return env;
  }

  public String getUrl() {
    return url;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  /** HTTP transport: bound on establishing the TCP connection. */
  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  /** HTTP transport: bound on one JSON-RPC round trip, from send to full response body. */
  public Duration getRequestTimeout() {
    return requestTimeout;
  }

  private static Duration positive(Duration value, Duration fallback, String field) {
    if (value == null) {
      return fallback;
    }
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(field + " must be positive, got " + value);
    }
    return value;
  }

  /** Connect to a STDIO MCP server. Example: {@code stdio("calculator", "npx", "-y", "mcp-calculator")}. */
  public static McpServerSpec stdio(String name, String... command) {
    return builder().withName(name).withTransport(Transport.STDIO).withCommand(List.of(command)).build();
  }

  /** Connect to an HTTP MCP server. Example: {@code http("everything", "http://localhost:3000/mcp")}. */
  public static McpServerSpec http(String name, String url) {
    return builder().withName(name).withTransport(Transport.HTTP).withUrl(url).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private Transport transport;
    private List<String> command;
    private Map<String, String> env;
    private String url;
    private Map<String, String> headers;
    private Duration connectTimeout;
    private Duration requestTimeout;

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withTransport(Transport transport) {
      this.transport = transport;
      return this;
    }

    public Builder withCommand(List<String> command) {
      this.command = command;
      return this;
    }

    public Builder withEnv(Map<String, String> env) {
      this.env = env;
      return this;
    }

    public Builder withUrl(String url) {
      this.url = url;
      return this;
    }

    public Builder withHeaders(Map<String, String> headers) {
      this.headers = headers;
      return this;
    }

    public Builder withConnectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder withRequestTimeout(Duration requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    public McpServerSpec build() {
      return new McpServerSpec(this);
    }
  }
}
