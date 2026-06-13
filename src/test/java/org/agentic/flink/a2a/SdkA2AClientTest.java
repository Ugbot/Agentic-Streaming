package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies the SDK adapter wiring that needs no live peer: ServiceLoader discovery resolves the SDK
 * factory, a pinned-endpoint client builds (synthesizing its card offline), and unsupported
 * transports fail loudly. Live send/get/cancel are exercised by gateway integration tests.
 */
class SdkA2AClientTest {

  private RemoteAgentSpec pinned(A2ATransport transport) {
    return RemoteAgentSpec.endpoint("peer", "http://localhost:65535/a2a", transport);
  }

  @Test
  @DisplayName("discovering() resolves the SDK factory and builds an SdkA2AClient")
  void discoveryResolvesSdkFactory() {
    A2AClient client = A2AClientFactory.discovering().create(pinned(A2ATransport.JSONRPC));
    try {
      assertInstanceOf(SdkA2AClient.class, client);
      // Pinned endpoint -> card is synthesized offline; no network required.
      A2AAgentCard card = client.fetchCard();
      assertEquals("peer", card.getName());
      assertEquals(
          "http://localhost:65535/a2a", card.endpointFor(A2ATransport.JSONRPC).orElseThrow());
    } finally {
      client.close();
    }
  }

  @Test
  @DisplayName("non-JSONRPC transports are reported as unsupported, not silently downgraded")
  void grpcUnsupported() {
    assertThrows(A2AClientException.class, () -> new SdkA2AClient(pinned(A2ATransport.GRPC)));
  }
}
