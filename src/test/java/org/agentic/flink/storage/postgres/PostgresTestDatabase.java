package org.agentic.flink.storage.postgres;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A real PostgreSQL started through Testcontainers for the storage integration tests.
 *
 * <p>The project runs containers with rootless Podman. Testcontainers talks to Podman through its
 * Docker-compatible API socket, which must be exported as {@code DOCKER_HOST}
 * ({@code unix:///run/user/<uid>/podman/podman.sock}, started with {@code podman system service
 * --time=0}) or symlinked to {@code /run/user/<uid>/docker.sock}, which Testcontainers detects on
 * its own. Ryuk (the reaper side-car) needs a privileged container and Docker Hub short-name
 * resolution, which rootless Podman does not provide by default; export {@code
 * TESTCONTAINERS_RYUK_DISABLED=true} (the tests stop their containers explicitly).
 *
 * <p>Uses a plain {@link GenericContainer} (the module only declares {@code testcontainers} core,
 * not the {@code postgresql} module) and waits until a JDBC connection succeeds. If no container
 * runtime is reachable this helper throws; it never skips.
 */
public final class PostgresTestDatabase implements AutoCloseable {

  public static final String IMAGE =
      System.getProperty("agentic.test.postgres.image", "postgres:16-alpine");

  private final GenericContainer<?> container;
  private final String databaseName;
  private final String username;
  private final String password;

  private PostgresTestDatabase(
      GenericContainer<?> container, String databaseName, String username, String password) {
    this.container = container;
    this.databaseName = databaseName;
    this.username = username;
    this.password = password;
  }

  public static PostgresTestDatabase start() {
    requireContainerRuntime();
    String db = "agentic_" + UUID.randomUUID().toString().replace('-', '_');
    String user = "agentic_" + UUID.randomUUID().toString().substring(0, 8);
    String pass = UUID.randomUUID().toString();
    GenericContainer<?> pg =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withEnv("POSTGRES_DB", db)
            .withEnv("POSTGRES_USER", user)
            .withEnv("POSTGRES_PASSWORD", pass)
            .withExposedPorts(5432)
            .waitingFor(
                Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    pg.start();
    PostgresTestDatabase database = new PostgresTestDatabase(pg, db, user, pass);
    database.awaitJdbc();
    return database;
  }

  private void awaitJdbc() {
    long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try (Connection c = DriverManager.getConnection(jdbcUrl(), username, password)) {
        if (c.isValid(5)) {
          return;
        }
      } catch (Exception e) {
        last = e;
        try {
          Thread.sleep(250);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("interrupted waiting for PostgreSQL", ie);
        }
      }
    }
    container.stop();
    throw new IllegalStateException("PostgreSQL container never accepted JDBC connections", last);
  }

  private static void requireContainerRuntime() {
    try {
      DockerClientFactory.instance().client();
    } catch (RuntimeException e) {
      String uid = System.getProperty("user.name");
      Path podmanSock =
          Paths.get(
              System.getenv().getOrDefault("XDG_RUNTIME_DIR", "/run/user/1000"),
              "podman",
              "podman.sock");
      throw new IllegalStateException(
          "No container runtime reachable for Testcontainers (user="
              + uid
              + ", DOCKER_HOST="
              + System.getenv("DOCKER_HOST")
              + ", podman socket "
              + podmanSock
              + (Files.exists(podmanSock) ? " exists" : " missing")
              + "). Start rootless Podman's API with `podman system service --time=0` and export"
              + " DOCKER_HOST=unix://"
              + podmanSock
              + " (and TESTCONTAINERS_RYUK_DISABLED=true if Ryuk cannot be pulled)."
              + " This test requires a real PostgreSQL and does not skip. Cause: "
              + e.getMessage(),
          e);
    }
  }

  /** Connection config in the shape every Postgres-backed store in this module accepts. */
  public Map<String, String> storeConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("postgres.url", jdbcUrl());
    config.put("postgres.user", username);
    config.put("postgres.password", password);
    config.put("postgres.pool.max.size", "16");
    config.put("postgres.pool.min.idle", "1");
    config.put("postgres.auto.create.tables", "true");
    return config;
  }

  public String jdbcUrl() {
    return "jdbc:postgresql://"
        + container.getHost()
        + ":"
        + container.getMappedPort(5432)
        + "/"
        + databaseName;
  }

  @Override
  public void close() {
    container.stop();
  }
}
