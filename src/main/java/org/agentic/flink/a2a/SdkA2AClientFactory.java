package org.agentic.flink.a2a;

/**
 * Production {@link A2AClientFactory}: builds {@link SdkA2AClient}s backed by the official
 * {@code a2a-java} SDK.
 *
 * <p>Registered as a {@link java.util.ServiceLoader} provider in {@code
 * META-INF/services/org.agentic.flink.a2a.A2AClientFactory}, so {@link
 * A2AClientFactory#discovering()} resolves it automatically when the SDK is on the classpath. Has a
 * public no-arg constructor for ServiceLoader. Requires the optional {@code a2a-java-sdk-client}
 * dependency; without it, instantiation/use fails with a clear {@link NoClassDefFoundError} surfaced
 * through the discovering factory's error message.
 */
public final class SdkA2AClientFactory implements A2AClientFactory {
  private static final long serialVersionUID = 1L;

  public SdkA2AClientFactory() {}

  @Override
  public A2AClient create(RemoteAgentSpec spec) {
    return new SdkA2AClient(spec);
  }
}
