package org.jagentic.gateway;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A name server for tests. A name under {@code .rebind.test} answers loopback the first time it is
 * asked, and {@link #later} after that, which is what a rebinding server does. Everything else goes to
 * the system's resolver. The JVM's own cache is off in tests ({@code sun.net.inetaddr.ttl=0}).
 */
public final class RebindingResolverProvider extends InetAddressResolverProvider {

  /** What a rebinding name answers after its first answer. */
  public static volatile InetAddress later;

  private static final Map<String, AtomicInteger> ASKED = new ConcurrentHashMap<>();

  @Override
  public InetAddressResolver get(Configuration configuration) {
    InetAddressResolver system = configuration.builtinResolver();
    return new InetAddressResolver() {
      @Override
      public Stream<InetAddress> lookupByName(String host, LookupPolicy policy) throws UnknownHostException {
        String h = host.toLowerCase(java.util.Locale.ROOT);
        if (h.endsWith(".rebind.test")) {
          int n = ASKED.computeIfAbsent(h, k -> new AtomicInteger()).incrementAndGet();
          InetAddress then = later;
          return Stream.of(n == 1 || then == null
              ? InetAddress.getByAddress(h, new byte[] {127, 0, 0, 1})
              : InetAddress.getByAddress(h, then.getAddress()));
        }
        return system.lookupByName(host, policy);
      }

      @Override
      public String lookupByAddress(byte[] addr) throws UnknownHostException {
        return system.lookupByAddress(addr);
      }
    };
  }

  @Override
  public String name() {
    return "rebinding-test-resolver";
  }
}
