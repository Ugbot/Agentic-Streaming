package org.jagentic.core.net;

import java.util.function.UnaryOperator;

/**
 * The operator's egress policy, applied to every HTTP tool, A2A peer and model call before anything
 * is sent. Private addresses and the metadata address are refused unless the operator says otherwise,
 * so a document that names an internal URL does not reach it. The settings are the operator's, not
 * the document's: {@code AGENTIC_EGRESS_ALLOW_HOSTS} (exact names or {@code *.suffix}, comma
 * separated; empty means any public host) and {@code AGENTIC_EGRESS_ALLOW_PRIVATE}, or the system
 * properties {@code agentic.egress.allow_hosts} and {@code agentic.egress.allow_private}.
 *
 * <p>A check resolves the host and the client resolves it again to connect, so a name that changes
 * between the two gets through. Redirects are not followed.
 */
public final class Egress {

  private Egress() {}

  /** A URL the policy refuses. Nothing was sent, so it is a definite failure. */
  public static final class Blocked extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public Blocked(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** The policy these settings describe; {@code settings} maps a property name to its value. */
  public static OutboundUrlPolicy policy(UnaryOperator<String> settings) {
    OutboundUrlPolicy policy = OutboundUrlPolicy.fromAllowlist(settings.apply("agentic.egress.allow_hosts"));
    return Boolean.parseBoolean(settings.apply("agentic.egress.allow_private"))
        ? policy.allowingPrivateAddresses() : policy;
  }

  /** The policy of this process, read on each call. */
  public static OutboundUrlPolicy current() {
    return policy(key -> {
      String set = System.getProperty(key);
      return set != null ? set : System.getenv(key.toUpperCase().replace('.', '_'));
    });
  }

  /** Throws {@link Blocked} for a URL the operator's policy refuses. */
  public static void check(String url) {
    try {
      current().validate(url);
    } catch (OutboundUrlPolicy.BlockedUrlException e) {
      throw new Blocked("blocked by egress policy: " + e.getMessage(), e);
    }
  }
}
