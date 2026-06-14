package org.agentic.flink.example.banking.safety;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks whether the customer's identity has been verified in a session, keyed by A2A {@code
 * contextId}. Consulted by {@link AuthorizationToolGuard} to gate high-risk actions.
 *
 * <p>Runtime-only state: the backing map is {@code transient} and rebuilt empty after
 * deserialization, so verification is never carried across a restart (a verified flag must be
 * re-established each run) and sessions stay isolated by {@code contextId}.
 */
public final class SessionAuthState implements Serializable {
  private static final long serialVersionUID = 1L;

  private transient volatile Map<String, Boolean> verified;

  public boolean isVerified(String contextId) {
    return contextId != null && Boolean.TRUE.equals(map().get(contextId));
  }

  public void markVerified(String contextId) {
    if (contextId != null) {
      map().put(contextId, Boolean.TRUE);
    }
  }

  public void reset(String contextId) {
    if (contextId != null) {
      map().remove(contextId);
    }
  }

  private Map<String, Boolean> map() {
    Map<String, Boolean> m = verified;
    if (m == null) {
      synchronized (this) {
        if (verified == null) {
          verified = new ConcurrentHashMap<>();
        }
        m = verified;
      }
    }
    return m;
  }
}
