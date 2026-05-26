package org.agentic.flink.tools.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class McpServerSpecTest {

  @Test
  @DisplayName("stdio() spec round-trips with command + env")
  void stdioRoundTrips() throws Exception {
    String name = "calc-" + UUID.randomUUID();
    McpServerSpec original =
        McpServerSpec.builder()
            .withName(name)
            .withTransport(McpServerSpec.Transport.STDIO)
            .withCommand(List.of("npx", "-y", "mcp-server-calculator"))
            .withEnv(Map.of("LOG_LEVEL", "debug", "API_KEY", "test"))
            .build();
    Object restored = roundTrip(original);
    assertInstanceOf(McpServerSpec.class, restored);
    McpServerSpec r = (McpServerSpec) restored;
    assertEquals(name, r.getName());
    assertEquals(McpServerSpec.Transport.STDIO, r.getTransport());
    assertEquals(List.of("npx", "-y", "mcp-server-calculator"), r.getCommand());
    assertEquals("debug", r.getEnv().get("LOG_LEVEL"));
  }

  @Test
  @DisplayName("http() spec round-trips with headers")
  void httpRoundTrips() throws Exception {
    String name = "hosted-" + UUID.randomUUID();
    McpServerSpec original =
        McpServerSpec.builder()
            .withName(name)
            .withTransport(McpServerSpec.Transport.HTTP)
            .withUrl("https://mcp.example.com/v1")
            .withHeaders(Map.of("Authorization", "Bearer abc123"))
            .build();
    Object restored = roundTrip(original);
    McpServerSpec r = (McpServerSpec) restored;
    assertEquals(McpServerSpec.Transport.HTTP, r.getTransport());
    assertEquals("https://mcp.example.com/v1", r.getUrl());
    assertEquals("Bearer abc123", r.getHeaders().get("Authorization"));
  }

  @Test
  @DisplayName("STDIO without command is rejected")
  void stdioRequiresCommand() {
    assertThrows(
        IllegalArgumentException.class,
        () -> McpServerSpec.builder()
            .withName("x").withTransport(McpServerSpec.Transport.STDIO).build());
  }

  @Test
  @DisplayName("HTTP without url is rejected")
  void httpRequiresUrl() {
    assertThrows(
        IllegalArgumentException.class,
        () -> McpServerSpec.builder()
            .withName("x").withTransport(McpServerSpec.Transport.HTTP).build());
  }

  private static Object roundTrip(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(obj);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return ois.readObject();
    }
  }
}
