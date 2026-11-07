package org.jagentic.core;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Who may resume a suspended turn or settle an unknown outcome ({@code policies.approvers}). Deny by
 * default: naming nobody means nobody can, and a {@code user_id} with no principal is nobody.
 */
public record Approvers(Set<String> subjects, Set<String> roles) {

  public static final Approvers NONE = new Approvers(Set.of(), Set.of());

  public static Approvers ofRoles(String... roles) {
    return new Approvers(Set.of(), Set.of(roles));
  }

  /** Reads {@code policies.approvers}; none when the block is absent. */
  public static Approvers fromPolicies(Map<String, Object> policies) {
    if (policies == null || !(policies.get("approvers") instanceof Map<?, ?> a)) {
      return NONE;
    }
    return new Approvers(names(a.get("subjects")), names(a.get("roles")));
  }

  private static Set<String> names(Object listed) {
    return listed instanceof List<?> l ? Set.copyOf(l.stream().map(String::valueOf).toList()) : Set.of();
  }

  /** Why this principal may not answer a turn of {@code tenant}, or null when it may. */
  public String problem(Principal who, String tenant) {
    if (who == null) {
      return "a signal needs an authenticated principal";
    }
    if (tenant != null && !tenant.equals(who.tenant())) {
      return who.subject() + " is not of this conversation's tenant";
    }
    if (subjects.contains(who.subject()) || !Collections.disjoint(roles, who.roles())) {
      return null;
    }
    return who.subject() + " is not an approver";
  }
}
