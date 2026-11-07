package org.jagentic.core;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Who a turn or a signal is authenticated as. Whatever builds one vouches for it: the gateway from a
 * verified token, or the program embedding the runtime. On the log it keeps a turn's identity across
 * a resume (spec section 11).
 */
public record Principal(String subject, String tenant, List<String> roles, String issuer)
    implements Serializable {

  public Principal {
    if (subject == null || subject.isBlank()) {
      throw new IllegalArgumentException("a principal needs a subject");
    }
    roles = roles == null ? List.of() : List.copyOf(roles);
  }

  public static Principal of(String subject, String tenant, String... roles) {
    return new Principal(subject, tenant, List.of(roles), null);
  }

  /** The wire form. What is not set is left out. */
  public Map<String, Object> toMap() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("subject", subject);
    if (tenant != null) {
      m.put("tenant", tenant);
    }
    if (!roles.isEmpty()) {
      m.put("roles", roles);
    }
    if (issuer != null) {
      m.put("issuer", issuer);
    }
    return m;
  }

  /** From the wire form, or null when there is none. */
  public static Principal fromMap(Object wire) {
    if (!(wire instanceof Map<?, ?> m)) {
      return null;
    }
    List<String> roles = m.get("roles") instanceof List<?> l ? l.stream().map(String::valueOf).toList() : List.of();
    return new Principal(text(m.get("subject")), text(m.get("tenant")), roles, text(m.get("issuer")));
  }

  private static String text(Object o) {
    return o == null ? null : String.valueOf(o);
  }
}
