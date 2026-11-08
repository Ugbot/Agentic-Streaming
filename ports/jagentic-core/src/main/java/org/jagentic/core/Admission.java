package org.jagentic.core;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Who may add a turn to a conversation (spec section 11). A conversation belongs to the principal of
 * its first {@code turn_received}, its tenant and its subject. {@code policies.sharing} lets others
 * of the same tenant in: every one of them with {@code tenant: true}, or those with one of {@code
 * roles}. Nobody outside the tenant, and no turn without a principal, is ever let in.
 *
 * <p>With a {@code verifier}, a principal is believed only with the gateway's proof for that very
 * turn ({@link org.jagentic.core.identity.PrincipalSeal}).
 */
public record Admission(boolean tenantWide, Set<String> roles,
                        org.jagentic.core.identity.PrincipalSeal.Verifier verifier) implements Serializable {

  /** Only the principal that started a conversation. */
  public static final Admission OWNER_ONLY = new Admission(false, Set.of(), null);

  public Admission {
    roles = roles == null ? Set.of() : Set.copyOf(roles);
  }

  public Admission(boolean tenantWide, Set<String> roles) {
    this(tenantWide, roles, null);
  }

  /** The same rule, believing a principal only with a proof these keys verify; null for none. */
  public Admission withVerifier(org.jagentic.core.identity.PrincipalSeal.Verifier v) {
    return new Admission(tenantWide, roles, v);
  }

  /** Reads {@code policies.sharing}; owner only when the block is absent. */
  public static Admission fromPolicies(Map<String, Object> policies) {
    if (policies == null || !(policies.get("sharing") instanceof Map<?, ?> s)) {
      return OWNER_ONLY;
    }
    Set<String> roles = s.get("roles") instanceof List<?> l
        ? Set.copyOf(l.stream().map(String::valueOf).toList()) : Set.of();
    return new Admission(Boolean.TRUE.equals(s.get("tenant")), roles);
  }

  /**
   * Why {@code who} may not add a turn to the conversation {@code owner} started, or null when it
   * may. A conversation started with no principal has no owner, and no check.
   */
  public String problem(String conversationId, Principal owner, Principal who) {
    if (owner == null) {
      return null;
    }
    if (who == null || !Objects.equals(owner.tenant(), who.tenant())) {
      return "conversation " + conversationId + " belongs to another tenant";
    }
    if (owner.subject().equals(who.subject()) || tenantWide || !Collections.disjoint(roles, who.roles())) {
      return null;
    }
    return "conversation " + conversationId + " belongs to another subject";
  }
}
