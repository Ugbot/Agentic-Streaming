package org.agentic.flink.a2a.gateway;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.agentic.flink.a2a.bridge.A2ABridge;
import org.agentic.flink.a2a.bridge.A2ABridgeFactory;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI producer for the gateway-side bridge connector, built from {@link GatewayConfig} (transport
 * selected by {@code a2a.bridge.transport}). A single application-scoped connector is shared by the
 * {@link AgenticFlinkAgentExecutor}; it is closed on shutdown via the {@link Disposes} method.
 */
@ApplicationScoped
public class BridgeProducer {
  private static final Logger LOG = LoggerFactory.getLogger(BridgeProducer.class);

  @Produces
  @Singleton
  public A2AGatewayConnector connector(GatewayConfig config) {
    A2ABridge bridge = A2ABridgeFactory.create(config.raw());
    try {
      A2AGatewayConnector connector = bridge.openGateway();
      LOG.info("A2A gateway bridge open (transport={})", bridge.transport());
      return connector;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to open A2A bridge connector", e);
    }
  }

  public void close(@Disposes A2AGatewayConnector connector) {
    connector.close();
    LOG.info("A2A gateway bridge connector closed");
  }
}
