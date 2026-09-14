package org.agentic.flink.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.agentic.flink.net.OutboundUrlPolicy.BlockedUrlException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class OutboundUrlPolicyTest {

  private final Random rnd = new Random();

  private static OutboundUrlPolicy pinned(String host, String... addrs) {
    return OutboundUrlPolicy.defaults()
        .withResolver(
            h -> {
              if (!h.equals(host)) {
                throw new UnknownHostException(h);
              }
              InetAddress[] out = new InetAddress[addrs.length];
              for (int i = 0; i < addrs.length; i++) {
                out[i] = InetAddress.getByName(addrs[i]);
              }
              return out;
            });
  }

  private String randomPrivateV4() {
    switch (rnd.nextInt(7)) {
      case 0:
        return "10." + oct() + "." + oct() + "." + oct();
      case 1:
        return "172." + (16 + rnd.nextInt(16)) + "." + oct() + "." + oct();
      case 2:
        return "192.168." + oct() + "." + oct();
      case 3:
        return "127." + oct() + "." + oct() + "." + oct();
      case 4:
        return "169.254." + oct() + "." + oct();
      case 5:
        return "100." + (64 + rnd.nextInt(64)) + "." + oct() + "." + oct();
      default:
        return "0." + oct() + "." + oct() + "." + oct();
    }
  }

  private String randomPublicV4() {
    while (true) {
      int o1 = 1 + rnd.nextInt(222);
      int o2 = oct();
      String ip = o1 + "." + o2 + "." + oct() + "." + oct();
      if (o1 == 10 || o1 == 127 || o1 == 0) {
        continue;
      }
      if (o1 == 172 && o2 >= 16 && o2 <= 31) {
        continue;
      }
      if (o1 == 192 && (o2 == 168 || o2 == 0)) {
        continue;
      }
      if (o1 == 169 && o2 == 254) {
        continue;
      }
      if (o1 == 100 && o2 >= 64 && o2 <= 127) {
        continue;
      }
      if (o1 == 198 && (o2 == 18 || o2 == 19)) {
        continue;
      }
      return ip;
    }
  }

  private int oct() {
    return rnd.nextInt(256);
  }

  @RepeatedTest(50)
  void literalPrivateAddressesAreBlocked() {
    String ip = randomPrivateV4();
    assertFalse(OutboundUrlPolicy.defaults().isAllowed("http://" + ip + "/x"), ip);
  }

  @RepeatedTest(50)
  void literalPublicAddressesAreAllowed() {
    String ip = randomPublicV4();
    assertTrue(OutboundUrlPolicy.defaults().isAllowed("https://" + ip + ":8443/y?q=1"), ip);
  }

  @RepeatedTest(20)
  void hostResolvingToAnyPrivateAddressIsBlocked() {
    String host = "h" + UUID.randomUUID().toString().substring(0, 8) + ".example";
    String pub = randomPublicV4();
    String priv = randomPrivateV4();
    OutboundUrlPolicy p =
        rnd.nextBoolean() ? pinned(host, pub, priv) : pinned(host, priv, pub);
    BlockedUrlException e =
        assertThrows(BlockedUrlException.class, () -> p.validate("http://" + host + "/"));
    assertTrue(e.getMessage().contains(priv), e.getMessage());
  }

  @RepeatedTest(20)
  void hostResolvingOnlyToPublicAddressesIsAllowed() throws Exception {
    String host = "ok" + UUID.randomUUID().toString().substring(0, 8) + ".example";
    OutboundUrlPolicy p = pinned(host, randomPublicV4(), randomPublicV4());
    assertEquals(host, p.validate("https://" + host + "/path").getHost());
  }

  @Test
  void metadataAddressIsBlocked() {
    assertFalse(OutboundUrlPolicy.defaults().isAllowed("http://169.254.169.254/latest/meta-data/"));
    assertTrue(OutboundUrlPolicy.isForbiddenAddress(addr("169.254.169.254")));
  }

  @Test
  void nonHttpSchemesAreBlocked() {
    for (String u :
        List.of(
            "file:///etc/passwd",
            "ftp://1.2.3.4/x",
            "gopher://1.2.3.4",
            "javascript:alert(1)",
            "//1.2.3.4/x",
            "1.2.3.4/x",
            "")) {
      assertFalse(OutboundUrlPolicy.defaults().isAllowed(u), u);
    }
  }

  @Test
  void userinfoIsBlocked() {
    assertFalse(OutboundUrlPolicy.defaults().isAllowed("http://user:pw@8.8.8.8/"));
  }

  @Test
  void ipv6PrivateFormsAreBlocked() {
    for (String a :
        List.of("::1", "::", "fe80::1", "fc00::1", "fd12:3456::1", "::ffff:10.0.0.1",
            "::ffff:169.254.169.254", "::10.0.0.1", "64:ff9b::a00:1", "2002:c0a8:1::")) {
      assertTrue(OutboundUrlPolicy.isForbiddenAddress(addr(a)), a);
      assertFalse(OutboundUrlPolicy.defaults().isAllowed("http://[" + a + "]/"), a);
    }
    assertFalse(OutboundUrlPolicy.isForbiddenAddress(addr("2606:4700:4700::1111")));
  }

  @Test
  void allowlistRestrictsHostsAndSupportsWildcards() throws Exception {
    String pub = randomPublicV4();
    OutboundUrlPolicy p =
        OutboundUrlPolicy.fromAllowlist("api.example.com, *.trusted.org")
            .withResolver(h -> new InetAddress[] {addr(pub)});
    assertTrue(p.isAllowed("https://api.example.com/v1"));
    assertTrue(p.isAllowed("https://a.b.trusted.org/"));
    assertFalse(p.isAllowed("https://trusted.org/"));
    assertFalse(p.isAllowed("https://evil.com/"));
    assertFalse(p.isAllowed("https://api.example.com.evil.com/"));
    assertEquals(Set.of("api.example.com", "*.trusted.org"), p.getAllowedHosts());
  }

  @Test
  void allowlistDoesNotBypassAddressCheck() {
    String priv = randomPrivateV4();
    OutboundUrlPolicy p =
        OutboundUrlPolicy.fromAllowlist("internal.example")
            .withResolver(h -> new InetAddress[] {addr(priv)});
    assertFalse(p.isAllowed("http://internal.example/"));
  }

  @Test
  void allowingPrivateAddressesIsExplicitOptIn() {
    String priv = randomPrivateV4();
    assertFalse(OutboundUrlPolicy.defaults().isAllowed("http://" + priv + "/"));
    assertTrue(OutboundUrlPolicy.defaults().allowingPrivateAddresses().isAllowed("http://" + priv + "/"));
    assertFalse(
        OutboundUrlPolicy.defaults().allowingPrivateAddresses().isAllowed("file:///" + priv),
        "scheme check still applies");
  }

  @Test
  void unresolvableHostIsBlocked() {
    OutboundUrlPolicy p =
        OutboundUrlPolicy.defaults()
            .withResolver(
                h -> {
                  throw new UnknownHostException(h);
                });
    assertFalse(p.isAllowed("http://nope.invalid/"));
  }

  private static InetAddress addr(String s) {
    try {
      return InetAddress.getByName(s);
    } catch (UnknownHostException e) {
      throw new AssertionError(e);
    }
  }
}
