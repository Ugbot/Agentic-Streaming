package org.agentic.flink.storage;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Base for stores that ship through Flink's job graph.
 *
 * <p>Java deserialization leaves every {@code transient} field null, so a store that was
 * {@link #initialize(Map) initialized} on the client and then serialized into an operator arrives
 * on the task side with its configuration but without its pool, client, or in-memory tables. This
 * class keeps the configuration map (which is serializable) and reopens the transient resources
 * from it on first use after deserialization, so the same instance works before and after the
 * trip. A store that was never initialized fails with a clear {@link IllegalStateException} rather
 * than a {@link NullPointerException} deep inside a driver.
 *
 * <p>Subclasses implement {@link #open(Map)} to build their transient state and call {@link
 * #ensureOpen()} at the top of every data method. A failure to reach the backend propagates from
 * {@code open}: there is no silent fallback here; degrading to memory is a workflow-level decision
 * ({@code on_unavailable: degrade}) made by the runtime that owns the store, not by the store.
 */
public abstract class ReopenableStore implements Serializable {
  private static final long serialVersionUID = 1L;

  private HashMap<String, String> config;
  private transient volatile boolean opened;
  private transient volatile boolean closed;

  /** Build every transient resource from {@code config}. Must throw if the backend is unreachable. */
  protected abstract void open(Map<String, String> config) throws Exception;

  public void initialize(Map<String, String> config) throws Exception {
    this.config = config == null ? new HashMap<>() : new HashMap<>(config);
    this.closed = false;
    openOnce();
  }

  /** Reopens transient state after deserialization; throws if the store was never initialized. */
  protected final void ensureOpen() {
    if (opened) {
      return;
    }
    synchronized (this) {
      if (opened) {
        return;
      }
      if (closed) {
        throw new IllegalStateException(getClass().getSimpleName() + " has been closed");
      }
      if (config == null) {
        throw new IllegalStateException(
            getClass().getSimpleName() + " used before initialize(config) was called");
      }
      try {
        openOnce();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalStateException(
            getClass().getSimpleName() + " could not reopen after deserialization: " + e.getMessage(),
            e);
      }
    }
  }

  private synchronized void openOnce() throws Exception {
    open(Collections.unmodifiableMap(config));
    opened = true;
  }

  /** Subclasses call this from {@code close()} after releasing their resources. */
  protected final synchronized void markClosed() {
    opened = false;
    closed = true;
  }

  /** True once {@link #initialize(Map)} ran on this instance or one of its serialized ancestors. */
  public final boolean isConfigured() {
    return config != null;
  }

  /** True while transient resources are live on this instance. */
  public final boolean isOpen() {
    return opened;
  }

  protected final Map<String, String> configuration() {
    return config == null ? Collections.emptyMap() : Collections.unmodifiableMap(config);
  }
}
