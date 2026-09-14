package org.agentic.flink.net;

import java.io.Serializable;
import java.net.IDN;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Egress policy for URLs that originate from untrusted input (model output, request bodies,
 * webhook registrations). A URL passes only if:
 *
 * <ul>
 *   <li>the scheme is {@code http} or {@code https};
 *   <li>it carries a host and no userinfo;
 *   <li>the host is on the allowlist when one is configured;
 *   <li>every address the host resolves to is publicly routable: loopback, link-local (including
 *       the cloud metadata address 169.254.169.254), RFC 1918, carrier-grade NAT (100.64/10),
 *       unspecified, multicast, broadcast, IPv6 unique-local and IPv4-mapped/compatible forms of
 *       any of those are denied.
 * </ul>
 *
 * <p>Callers that follow redirects must re-validate each {@code Location} with {@link #validate}
 * and stop after {@link #getMaxRedirects()} hops. Resolution runs through a pluggable
 * {@link Resolver} so tests can pin hostnames to addresses without touching DNS.
 */
public final class OutboundUrlPolicy implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Resolves a host to its addresses. The default delegates to {@link InetAddress#getAllByName}. */
  @FunctionalInterface
  public interface Resolver extends Serializable {
    InetAddress[] resolve(String host) throws UnknownHostException;
  }

  /** Thrown when a URL is rejected by the policy. */
  public static final class BlockedUrlException extends java.io.IOException {
    private static final long serialVersionUID = 1L;

    public BlockedUrlException(String message) {
      super(message);
    }
  }

  private static final Resolver SYSTEM_RESOLVER = InetAddress::getAllByName;

  private final Set<String> allowedHosts;
  private final boolean allowPrivateAddresses;
  private final int maxRedirects;
  private final Resolver resolver;

  private OutboundUrlPolicy(
      Set<String> allowedHosts, boolean allowPrivateAddresses, int maxRedirects, Resolver resolver) {
    this.allowedHosts = Collections.unmodifiableSet(new LinkedHashSet<>(allowedHosts));
    this.allowPrivateAddresses = allowPrivateAddresses;
    this.maxRedirects = Math.max(0, maxRedirects);
    this.resolver = Objects.requireNonNull(resolver, "resolver");
  }

  /** http/https only, private ranges denied, no host allowlist, at most five redirects. */
  public static OutboundUrlPolicy defaults() {
    return new OutboundUrlPolicy(Set.of(), false, 5, SYSTEM_RESOLVER);
  }

  /**
   * Builds a policy from a comma-separated host allowlist. Entries are exact hostnames or
   * {@code *.suffix} wildcards. A null or blank list means every public host is permitted.
   */
  public static OutboundUrlPolicy fromAllowlist(String commaSeparatedHosts) {
    OutboundUrlPolicy p = defaults();
    if (commaSeparatedHosts == null || commaSeparatedHosts.isBlank()) {
      return p;
    }
    Set<String> hosts = new LinkedHashSet<>();
    for (String h : commaSeparatedHosts.split(",")) {
      String t = h.trim().toLowerCase(Locale.ROOT);
      if (!t.isEmpty()) {
        hosts.add(t);
      }
    }
    return p.withAllowedHosts(hosts);
  }

  public OutboundUrlPolicy withAllowedHosts(Set<String> hosts) {
    Set<String> normalized = new LinkedHashSet<>();
    for (String h : hosts) {
      normalized.add(h.trim().toLowerCase(Locale.ROOT));
    }
    return new OutboundUrlPolicy(normalized, allowPrivateAddresses, maxRedirects, resolver);
  }

  /**
   * Permits loopback, private and link-local targets. Intended for local development and tests that
   * run an in-process HTTP server; never enable it on a deployment reachable by untrusted callers.
   */
  public OutboundUrlPolicy allowingPrivateAddresses() {
    return new OutboundUrlPolicy(allowedHosts, true, maxRedirects, resolver);
  }

  public OutboundUrlPolicy withMaxRedirects(int n) {
    return new OutboundUrlPolicy(allowedHosts, allowPrivateAddresses, n, resolver);
  }

  public OutboundUrlPolicy withResolver(Resolver r) {
    return new OutboundUrlPolicy(allowedHosts, allowPrivateAddresses, maxRedirects, r);
  }

  public Set<String> getAllowedHosts() {
    return allowedHosts;
  }

  public boolean isPrivateAddressesAllowed() {
    return allowPrivateAddresses;
  }

  public int getMaxRedirects() {
    return maxRedirects;
  }

  /**
   * Validates {@code url} and returns the parsed URI. Resolution happens on every call so that a
   * host whose DNS answer changed since a previous check is re-evaluated.
   */
  public URI validate(String url) throws BlockedUrlException {
    if (url == null || url.isBlank()) {
      throw new BlockedUrlException("url is required");
    }
    URI uri;
    try {
      uri = new URI(url.trim());
    } catch (java.net.URISyntaxException e) {
      throw new BlockedUrlException("malformed url: " + e.getMessage());
    }
    return validate(uri);
  }

  public URI validate(URI uri) throws BlockedUrlException {
    String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
    if (!scheme.equals("http") && !scheme.equals("https")) {
      throw new BlockedUrlException("scheme not allowed: " + (scheme.isEmpty() ? "(none)" : scheme));
    }
    if (uri.getRawUserInfo() != null) {
      throw new BlockedUrlException("userinfo in url is not allowed");
    }
    String host = uri.getHost();
    if (host == null || host.isEmpty()) {
      throw new BlockedUrlException("url has no host");
    }
    String asciiHost = normalizeHost(host);
    if (!allowedHosts.isEmpty() && !hostAllowed(asciiHost)) {
      throw new BlockedUrlException("host not on allowlist: " + asciiHost);
    }
    if (allowPrivateAddresses) {
      return uri;
    }
    InetAddress[] addresses;
    try {
      addresses = resolver.resolve(asciiHost);
    } catch (UnknownHostException e) {
      throw new BlockedUrlException("host does not resolve: " + asciiHost);
    }
    if (addresses == null || addresses.length == 0) {
      throw new BlockedUrlException("host does not resolve: " + asciiHost);
    }
    for (InetAddress a : addresses) {
      if (isForbiddenAddress(a)) {
        throw new BlockedUrlException(
            "host " + asciiHost + " resolves to a non-public address " + a.getHostAddress());
      }
    }
    return uri;
  }

  /** True if the policy would accept {@code url}; never throws. */
  public boolean isAllowed(String url) {
    try {
      validate(url);
      return true;
    } catch (BlockedUrlException e) {
      return false;
    }
  }

  private boolean hostAllowed(String host) {
    for (String pattern : allowedHosts) {
      if (pattern.startsWith("*.")) {
        String suffix = pattern.substring(1);
        if (host.endsWith(suffix) && host.length() > suffix.length()) {
          return true;
        }
      } else if (pattern.equals(host)) {
        return true;
      }
    }
    return false;
  }

  private static String normalizeHost(String host) {
    String h = host;
    if (h.startsWith("[") && h.endsWith("]")) {
      h = h.substring(1, h.length() - 1);
    }
    try {
      h = IDN.toASCII(h);
    } catch (IllegalArgumentException ignored) {
      // keep the raw form; resolution will decide
    }
    if (h.endsWith(".")) {
      h = h.substring(0, h.length() - 1);
    }
    return h.toLowerCase(Locale.ROOT);
  }

  /**
   * Whether {@code a} is anything other than a publicly routable unicast address. Covers IPv4,
   * IPv6, and IPv4 embedded in IPv6 (mapped {@code ::ffff:a.b.c.d} and compatible {@code ::a.b.c.d}).
   */
  public static boolean isForbiddenAddress(InetAddress a) {
    if (a == null) {
      return true;
    }
    if (a.isAnyLocalAddress()
        || a.isLoopbackAddress()
        || a.isLinkLocalAddress()
        || a.isSiteLocalAddress()
        || a.isMulticastAddress()) {
      return true;
    }
    byte[] b = a.getAddress();
    if (a instanceof Inet4Address || b.length == 4) {
      return isForbiddenV4(b);
    }
    if (a instanceof Inet6Address || b.length == 16) {
      int first = b[0] & 0xff;
      // fc00::/7 unique local
      if ((first & 0xfe) == 0xfc) {
        return true;
      }
      // IPv4-mapped ::ffff:0:0/96 and IPv4-compatible ::/96 (excluding :: and ::1 handled above)
      boolean leadingZeros = true;
      for (int i = 0; i < 10; i++) {
        if (b[i] != 0) {
          leadingZeros = false;
          break;
        }
      }
      if (leadingZeros) {
        boolean mapped = (b[10] & 0xff) == 0xff && (b[11] & 0xff) == 0xff;
        boolean compat = b[10] == 0 && b[11] == 0;
        if (mapped || compat) {
          return isForbiddenV4(new byte[] {b[12], b[13], b[14], b[15]});
        }
      }
      // 64:ff9b::/96 NAT64 well-known prefix
      if ((first) == 0x00 && (b[1] & 0xff) == 0x64 && (b[2] & 0xff) == 0xff && (b[3] & 0xff) == 0x9b) {
        boolean zeros = true;
        for (int i = 4; i < 12; i++) {
          if (b[i] != 0) {
            zeros = false;
            break;
          }
        }
        if (zeros) {
          return isForbiddenV4(new byte[] {b[12], b[13], b[14], b[15]});
        }
      }
      // 2002::/16 6to4 embeds an IPv4 address in bytes 2..5
      if (first == 0x20 && (b[1] & 0xff) == 0x02) {
        return isForbiddenV4(new byte[] {b[2], b[3], b[4], b[5]});
      }
      return false;
    }
    return true;
  }

  private static boolean isForbiddenV4(byte[] b) {
    int o1 = b[0] & 0xff;
    int o2 = b[1] & 0xff;
    if (o1 == 0) { // 0.0.0.0/8
      return true;
    }
    if (o1 == 10) { // 10/8
      return true;
    }
    if (o1 == 127) { // 127/8
      return true;
    }
    if (o1 == 169 && o2 == 254) { // 169.254/16 link-local incl. 169.254.169.254
      return true;
    }
    if (o1 == 172 && o2 >= 16 && o2 <= 31) { // 172.16/12
      return true;
    }
    if (o1 == 192 && o2 == 168) { // 192.168/16
      return true;
    }
    if (o1 == 100 && o2 >= 64 && o2 <= 127) { // 100.64/10 carrier-grade NAT
      return true;
    }
    if (o1 == 192 && o2 == 0 && (b[2] & 0xff) == 0) { // 192.0.0/24 protocol assignments
      return true;
    }
    if (o1 == 198 && (o2 == 18 || o2 == 19)) { // 198.18/15 benchmarking
      return true;
    }
    if (o1 >= 224) { // multicast + reserved + broadcast
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return "OutboundUrlPolicy{allowedHosts="
        + allowedHosts
        + ", allowPrivateAddresses="
        + allowPrivateAddresses
        + ", maxRedirects="
        + maxRedirects
        + '}';
  }
}
