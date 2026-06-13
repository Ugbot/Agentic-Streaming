package org.agentic.flink.a2a;

import java.io.Serializable;

/**
 * Serializable factory that builds a live {@link A2AClient} from a {@link RemoteAgentSpec} on the
 * Flink task side.
 *
 * <p>The outbound tool and pipeline step hold a factory (which ships in the job graph) rather than a
 * client (which does not serialize). The default, {@link #discovering()}, resolves an
 * implementation via {@link java.util.ServiceLoader} at {@code create} time — the SDK adapter
 * registers itself as {@code org.agentic.flink.a2a.SdkA2AClientFactory}. Tests pass a lambda
 * returning a fake; because this interface extends {@link Serializable}, such lambdas serialize.
 */
@FunctionalInterface
public interface A2AClientFactory extends Serializable {

  /** Construct a client bound to the given peer spec. */
  A2AClient create(RemoteAgentSpec spec);

  /**
   * The default factory: a serializable indirection that, when invoked on the task side, discovers a
   * concrete {@link A2AClientFactory} provider via {@link java.util.ServiceLoader}. Throws a clear
   * error if no provider is on the classpath (i.e. the A2A SDK was excluded).
   */
  static A2AClientFactory discovering() {
    return DiscoveringA2AClientFactory.INSTANCE;
  }
}
