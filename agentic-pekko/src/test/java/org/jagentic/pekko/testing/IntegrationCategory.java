package org.jagentic.pekko.testing;

/**
 * JUnit 4 category for the ScalaTest-based Pekko TCK specs; the vintage engine exposes it as the
 * tag {@code org.jagentic.pekko.testing.IntegrationCategory}, which the {@code integration-tests}
 * profile selects alongside the JUnit 5 {@code integration} tag.
 */
public interface IntegrationCategory {}
