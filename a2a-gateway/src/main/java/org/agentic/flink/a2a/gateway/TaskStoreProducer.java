package org.agentic.flink.a2a.gateway;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.agentic.flink.a2a.storage.A2ATaskStore;
import org.agentic.flink.a2a.storage.A2ATaskStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI producer for the gateway's {@link A2ATaskStore}: builds the backend selected by {@link
 * GatewayConfig#taskStoreBackend()} (default {@code memory}; {@code redis}/{@code postgres} opt-in)
 * via {@link A2ATaskStoreFactory}, as an application-scoped singleton so {@link A2AResource} can
 * persist the task lifecycle (submitted → working → completed/failed) and serve {@code tasks/get} /
 * {@code tasks/cancel} from it.
 */
@ApplicationScoped
public class TaskStoreProducer {

  private static final Logger LOG = LoggerFactory.getLogger(TaskStoreProducer.class);

  @Inject GatewayConfig config;

  @Produces
  @ApplicationScoped
  public A2ATaskStore taskStore() {
    try {
      A2ATaskStore store =
          A2ATaskStoreFactory.create(config.taskStoreBackend(), config.taskStoreConfig());
      LOG.info("Gateway task store: backend={}", config.taskStoreBackend());
      return store;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to initialize A2A task store backend=" + config.taskStoreBackend(), e);
    }
  }

  void dispose(@Disposes A2ATaskStore store) {
    try {
      store.close();
    } catch (Exception e) {
      LOG.warn("error closing task store: {}", e.toString());
    }
  }
}
