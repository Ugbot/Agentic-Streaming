package org.jagentic.gateway.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.Principal;

/**
 * Fixed tokens, for local development only. {@link org.jagentic.gateway.GatewayOptions} starts this
 * only when told to, and never on anything but loopback. Only digests are kept, and every entry is
 * compared in constant time, so neither a match nor its position shows in the timing.
 */
public final class StaticTokens implements Authenticator {

  private final Map<byte[], Principal> byDigest = new LinkedHashMap<>();

  /**
   * @param spec entries separated by {@code ;}, each {@code token=subject|tenant|role,role}. A token
   *     shorter than 16 characters is refused, so a placeholder cannot end up guarding anything
   */
  public static StaticTokens parse(String spec) {
    StaticTokens tokens = new StaticTokens();
    for (String entry : spec.split(";")) {
      if (entry.isBlank()) {
        continue;
      }
      int eq = entry.indexOf('=');
      if (eq < 16) {
        throw new IllegalArgumentException("a static token needs at least 16 characters and a principal");
      }
      String[] who = entry.substring(eq + 1).trim().split("\\|", -1);
      if (who.length < 2) {
        throw new IllegalArgumentException("a static token's principal is subject|tenant|roles");
      }
      List<String> roles = new ArrayList<>();
      if (who.length > 2) {
        for (String role : who[2].split(",")) {
          if (!role.isBlank()) {
            roles.add(role.trim());
          }
        }
      }
      tokens.byDigest.put(digest(entry.substring(0, eq).trim()),
          new Principal(who[0].trim(), who[1].trim(), roles, "static"));
    }
    if (tokens.byDigest.isEmpty()) {
      throw new IllegalArgumentException("no static tokens given");
    }
    return tokens;
  }

  @Override
  public Principal authenticate(String bearerToken) throws Unauthenticated {
    byte[] offered = digest(bearerToken);
    Principal match = null;
    for (Map.Entry<byte[], Principal> e : byDigest.entrySet()) {
      if (MessageDigest.isEqual(e.getKey(), offered)) {
        match = e.getValue();
      }
    }
    if (match == null) {
      throw new Unauthenticated("unknown static token");
    }
    return match;
  }

  private static byte[] digest(String token) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
