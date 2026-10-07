package org.agentic.flink.storage.vector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A real Qdrant started through Testcontainers for {@link QdrantVectorStoreIT}.
 *
 * <p>Runs through the same rootless Podman socket as the PostgreSQL helper (see {@code
 * PostgresTestDatabase} for the {@code DOCKER_HOST} and {@code TESTCONTAINERS_RYUK_DISABLED}
 * setup). Readiness is the REST {@code /readyz} endpoint; the store itself talks gRPC on 6334. If
 * no container runtime is reachable this helper throws; it never skips.
 */
public final class QdrantTestServer implements AutoCloseable {

  public static final String IMAGE =
      System.getProperty("agentic.test.qdrant.image", "qdrant/qdrant:v1.13.2");

  private static final int REST_PORT = 6333;
  private static final int GRPC_PORT = 6334;

  private final GenericContainer<?> container;

  private QdrantTestServer(GenericContainer<?> container) {
    this.container = container;
  }

  public static QdrantTestServer start() {
    requireContainerRuntime();
    GenericContainer<?> qdrant =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(REST_PORT, GRPC_PORT)
            .waitingFor(
                Wait.forHttp("/readyz")
                    .forPort(REST_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    qdrant.start();
    return new QdrantTestServer(qdrant);
  }

  private static void requireContainerRuntime() {
    try {
      DockerClientFactory.instance().client();
    } catch (RuntimeException e) {
      Path podmanSock =
          Paths.get(
              System.getenv().getOrDefault("XDG_RUNTIME_DIR", "/run/user/1000"),
              "podman",
              "podman.sock");
      throw new IllegalStateException(
          "No container runtime reachable for Testcontainers (DOCKER_HOST="
              + System.getenv("DOCKER_HOST")
              + ", podman socket "
              + podmanSock
              + (Files.exists(podmanSock) ? " exists" : " missing")
              + "). Start rootless Podman's API with `podman system service --time=0` and export"
              + " DOCKER_HOST=unix://"
              + podmanSock
              + " (and TESTCONTAINERS_RYUK_DISABLED=true if Ryuk cannot be pulled)."
              + " This test requires a real Qdrant and does not skip. Cause: "
              + e.getMessage(),
          e);
    }
  }

  public String host() {
    return container.getHost();
  }

  public int grpcPort() {
    return container.getMappedPort(GRPC_PORT);
  }

  /** Connection config in the shape {@link QdrantVectorStore#initialize(Map)} accepts. */
  public Map<String, String> storeConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("qdrant.host", host());
    config.put("qdrant.port", Integer.toString(grpcPort()));
    config.put("qdrant.use.tls", "false");
    return config;
  }

  @Override
  public void close() {
    container.stop();
  }
}
