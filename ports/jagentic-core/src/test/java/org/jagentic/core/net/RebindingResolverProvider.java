package org.jagentic.core.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A name server for tests. A name under {@code .rebind.test} answers a public address the first time
 * it is asked and loopback after that, which is what a rebinding server does. {@code pinned.test} and
 * {@code other.test} are loopback. Everything else goes to the system's resolver. The JVM's own cache
 * is off in tests ({@code sun.net.inetaddr.ttl=0}), as an attacker's TTL of zero would make it.
 */
public final class RebindingResolverProvider extends InetAddressResolverProvider {

  /** What a rebinding name answers first. */
  public static final String PUBLIC = "8.8.8.8";

  private static final Map<String, AtomicInteger> ASKED = new ConcurrentHashMap<>();

  /** How many times {@code host} was asked since the last {@link #reset}. */
  public static int asked(String host) {
    AtomicInteger n = ASKED.get(host);
    return n == null ? 0 : n.get();
  }

  public static void reset() {
    ASKED.clear();
  }

  @Override
  public InetAddressResolver get(Configuration configuration) {
    InetAddressResolver system = configuration.builtinResolver();
    return new InetAddressResolver() {
      @Override
      public Stream<InetAddress> lookupByName(String host, LookupPolicy policy) throws UnknownHostException {
        String h = host.toLowerCase(java.util.Locale.ROOT);
        if (h.endsWith(".rebind.test")) {
          int n = ASKED.computeIfAbsent(h, k -> new AtomicInteger()).incrementAndGet();
          return Stream.of(InetAddress.getByAddress(h, n == 1 ? bytes(PUBLIC) : bytes("127.0.0.1")));
        }
        if (h.equals("pinned.test") || h.equals("other.test")) {
          ASKED.computeIfAbsent(h, k -> new AtomicInteger()).incrementAndGet();
          return Stream.of(InetAddress.getByAddress(h, bytes("127.0.0.1")));
        }
        return system.lookupByName(host, policy);
      }

      @Override
      public String lookupByAddress(byte[] addr) throws UnknownHostException {
        return system.lookupByAddress(addr);
      }
    };
  }

  private static byte[] bytes(String dotted) {
    String[] p = dotted.split("\\.");
    return new byte[] {(byte) Integer.parseInt(p[0]), (byte) Integer.parseInt(p[1]),
        (byte) Integer.parseInt(p[2]), (byte) Integer.parseInt(p[3])};
  }

  @Override
  public String name() {
    return "rebinding-test-resolver";
  }
}
