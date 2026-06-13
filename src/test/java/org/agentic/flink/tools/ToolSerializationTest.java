package org.agentic.flink.tools;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.agentic.flink.a2a.A2AClient;
import org.agentic.flink.a2a.A2AClientFactory;
import org.agentic.flink.a2a.A2AToolExecutor;
import org.agentic.flink.a2a.A2ATransport;
import org.agentic.flink.a2a.RemoteAgentSpec;
import org.agentic.flink.web.ExtractLinksTool;
import org.agentic.flink.web.WebFetchTool;
import org.agentic.flink.web.WebToolkitOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Built-in {@link ToolExecutor}s are held inside Flink operators and serialized at job submit, so a
 * non-transient non-serializable field (HTTP client, parser, SDK client) kills the job at
 * ClosureCleaner. Today the banking KB tools + CS client hit exactly this; these round-trips guard
 * the convention for the core operator-held tools (HTTP-client holders are the prime suspects).
 */
class ToolSerializationTest {

  @SuppressWarnings("unchecked")
  private static <T> T roundTrip(T o) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(o);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return (T) ois.readObject();
    }
  }

  @Test
  @DisplayName("WebFetchTool / ExtractLinksTool round-trip (transient HttpClient/parser)")
  void webToolsSerializable() throws Exception {
    WebToolkitOptions opts = WebToolkitOptions.defaults();
    assertInstanceOf(WebFetchTool.class, roundTrip(new WebFetchTool(opts)));
    assertInstanceOf(ExtractLinksTool.class, roundTrip(new ExtractLinksTool(opts)));
  }

  @Test
  @DisplayName("A2AToolExecutor round-trips (transient A2AClient + executor pool)")
  void a2aToolSerializable() throws Exception {
    RemoteAgentSpec spec = RemoteAgentSpec.endpoint("peer", "http://localhost:9001", A2ATransport.JSONRPC);
    // A serializable factory (the SPI extends Serializable). A no-capture lambda — NOT an anonymous
    // inner class, which would capture the (non-serializable) enclosing test instance. Never invoked.
    A2AClientFactory factory =
        (RemoteAgentSpec s) -> {
          throw new UnsupportedOperationException("not used in serialization test");
        };
    assertInstanceOf(A2AToolExecutor.class, roundTrip(new A2AToolExecutor(spec, factory)));
  }
}
