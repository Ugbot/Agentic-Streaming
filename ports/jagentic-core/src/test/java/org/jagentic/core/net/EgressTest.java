package org.jagentic.core.net;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** The operator's settings are the only thing that loosens egress. */
class EgressTest {

  private static OutboundUrlPolicy policy(Map<String, String> settings) throws Exception {
    InetAddress local = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    InetAddress world = InetAddress.getByAddress(new byte[] {93, (byte) 184, (byte) 216, 34});
    return Egress.policy(settings::get)
        .withResolver(host -> new InetAddress[] {host.contains("internal") || host.startsWith("169.") ? local : world});
  }

  @Test
  void aPrivateAddressIsRefusedByDefault() throws Exception {
    OutboundUrlPolicy p = policy(Map.of());
    assertThrows(OutboundUrlPolicy.BlockedUrlException.class, () -> p.validate("http://internal.corp/x"));
    assertThrows(OutboundUrlPolicy.BlockedUrlException.class, () -> p.validate("http://169.254.169.254/"));
    assertThrows(OutboundUrlPolicy.BlockedUrlException.class, () -> p.validate("file:///etc/passwd"));
    assertDoesNotThrow(() -> p.validate("https://example.com/"));
  }

  @Test
  void theAllowlistNarrowsAndPrivateNeedsItsOwnSetting() throws Exception {
    OutboundUrlPolicy hosts = policy(Map.of("agentic.egress.allow_hosts", "internal.corp,*.example.com"));
    assertThrows(OutboundUrlPolicy.BlockedUrlException.class, () -> hosts.validate("https://other.org/"));
    assertThrows(OutboundUrlPolicy.BlockedUrlException.class, () -> hosts.validate("http://internal.corp/"));
    assertDoesNotThrow(() -> hosts.validate("https://a.example.com/"));
    OutboundUrlPolicy open = policy(Map.of("agentic.egress.allow_hosts", "internal.corp",
        "agentic.egress.allow_private", "true"));
    assertDoesNotThrow(() -> open.validate("http://internal.corp/"));
    assertThrows(OutboundUrlPolicy.BlockedUrlException.class, () -> open.validate("http://169.254.169.254/"));
  }

  @Test
  void aBlockedUrlSaysSoWithoutTheCaller() {
    Egress.Blocked e = assertThrows(Egress.Blocked.class, () -> Egress.check("http://169.254.169.254/"));
    assertTrue(e.getMessage().contains("egress policy"));
  }
}
