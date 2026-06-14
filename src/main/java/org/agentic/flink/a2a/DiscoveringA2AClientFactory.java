package org.agentic.flink.a2a;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Default {@link A2AClientFactory}: resolves a concrete provider via {@link ServiceLoader} the
 * first time {@link #create(RemoteAgentSpec)} is invoked on the task side, then delegates to it.
 *
 * <p>Kept as a named singleton (rather than a lambda) so it serializes to a stable identity in the
 * job graph and re-resolves the provider after deserialization on each TaskManager. The production
 * provider is {@code SdkA2AClientFactory} (declared in {@code
 * META-INF/services/org.agentic.flink.a2a.A2AClientFactory}); any provider on the classpath that is
 * not this discovering shim is eligible.
 */
final class DiscoveringA2AClientFactory implements A2AClientFactory {
  private static final long serialVersionUID = 1L;

  static final DiscoveringA2AClientFactory INSTANCE = new DiscoveringA2AClientFactory();

  // Resolved lazily per JVM; transient so each TaskManager re-discovers after deserialization.
  private transient volatile A2AClientFactory delegate;

  private DiscoveringA2AClientFactory() {}

  @Override
  public A2AClient create(RemoteAgentSpec spec) {
    A2AClientFactory resolved = delegate;
    if (resolved == null) {
      synchronized (this) {
        if (delegate == null) {
          delegate = resolve();
        }
        resolved = delegate;
      }
    }
    A2AClient client = resolved.create(spec);
    // Harden every production client with retry + backoff + circuit breaker. Idempotent: a provider
    // that already returns a ResilientA2AClient is left untouched.
    return (client instanceof ResilientA2AClient) ? client : new ResilientA2AClient(client, spec);
  }

  private static A2AClientFactory resolve() {
    List<A2AClientFactory> providers = new ArrayList<>();
    Iterator<A2AClientFactory> it = ServiceLoader.load(A2AClientFactory.class).iterator();
    while (it.hasNext()) {
      try {
        A2AClientFactory provider = it.next();
        if (!(provider instanceof DiscoveringA2AClientFactory)) {
          providers.add(provider);
        }
      } catch (ServiceConfigurationError ignored) {
        // A provider whose class is absent (e.g. SDK excluded) — skip it.
      }
    }
    if (providers.isEmpty()) {
      throw new A2AClientException(
          "No A2AClientFactory provider found on the classpath. Add the A2A SDK "
              + "(io.github.a2asdk:a2a-java-sdk-client) so org.agentic.flink.a2a.SdkA2AClientFactory "
              + "is available, or supply an explicit factory via AgentBuilder.withA2AClientFactory(...).");
    }
    return providers.get(0);
  }

  // Preserve singleton identity across Java deserialization.
  private Object readResolve() {
    return INSTANCE;
  }
}
