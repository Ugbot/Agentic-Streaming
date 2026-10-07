package org.agentic.flink.testkit;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.TimeoutException;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * A Fluss cluster for the {@code @Tag("integration")} tests. When {@code FLUSS_BOOTSTRAP_SERVERS}
 * is set (an externally provisioned cluster, for example {@code docker-compose-fluss.yml}) the
 * tests use it and fail if it is unreachable. Otherwise ZooKeeper, one coordinator and one tablet
 * server are started through Testcontainers on the host network, each on a free port picked up
 * front: a Fluss client reconnects to the advertised listener addresses after bootstrap, so the
 * servers must advertise addresses that resolve from the test JVM, and host networking gives the
 * three processes and the JVM one address space without a user-defined container network (which
 * rootless Podman with the CNI backend cannot always create). Never skips: a missing container
 * runtime or an unreachable cluster is an {@link IllegalStateException}.
 */
public final class FlussTestCluster implements AutoCloseable {

  public static final String BOOTSTRAP_ENV = "FLUSS_BOOTSTRAP_SERVERS";
  public static final String FLUSS_IMAGE =
      System.getProperty("agentic.test.fluss.image", "docker.io/fluss/fluss:0.7.0");
  public static final String ZOOKEEPER_IMAGE =
      System.getProperty("agentic.test.zookeeper.image", "docker.io/library/zookeeper:3.9.2");

  private static final Duration STARTUP = Duration.ofMinutes(3);

  private final String bootstrap;
  private final List<GenericContainer<?>> containers;

  private FlussTestCluster(String bootstrap, List<GenericContainer<?>> containers) {
    this.bootstrap = bootstrap;
    this.containers = containers;
  }

  /** {@code host:port} of the coordinator's client listener. */
  public String bootstrapServers() {
    return bootstrap;
  }

  public static FlussTestCluster start() {
    String external = System.getenv(BOOTSTRAP_ENV);
    if (external != null && !external.isBlank()) {
      requireReachable(external);
      return new FlussTestCluster(external, List.of());
    }
    requireContainerRuntime();

    int zkPort = freePort();
    int coordinatorInternal = freePort();
    int coordinatorClient = freePort();
    int tabletInternal = freePort();
    int tabletClient = freePort();

    // The image ships a zoo.cfg (clientPort 2181, admin server on 8080), so it is replaced
    // instead of configured through ZOO_* variables, which the entrypoint only honours when no
    // zoo.cfg exists.
    GenericContainer<?> zookeeper =
        new GenericContainer<>(DockerImageName.parse(ZOOKEEPER_IMAGE))
            .withNetworkMode("host")
            .withCommand(
                "sh",
                "-c",
                "printf '%s\\n' 'dataDir=/data' 'dataLogDir=/datalog' 'tickTime=2000'"
                    + " 'clientPort="
                    + zkPort
                    + "' 'admin.enableServer=false' '4lw.commands.whitelist=*'"
                    + " > /conf/zoo.cfg && exec zkServer.sh start-foreground")
            .waitingFor(new LocalPortWait(zkPort))
            .withStartupTimeout(STARTUP);
    GenericContainer<?> coordinator =
        flussServer(
                "coordinatorServer",
                coordinatorClient,
                String.join(
                    "\n",
                    "zookeeper.address: localhost:" + zkPort,
                    "bind.listeners: INTERNAL://0.0.0.0:"
                        + coordinatorInternal
                        + ",CLIENT://0.0.0.0:"
                        + coordinatorClient,
                    "advertised.listeners: INTERNAL://localhost:"
                        + coordinatorInternal
                        + ",CLIENT://localhost:"
                        + coordinatorClient,
                    "internal.listener.name: INTERNAL",
                    "default.replication.factor: 1"))
            .dependsOn(zookeeper);
    GenericContainer<?> tablet =
        flussServer(
                "tabletServer",
                tabletClient,
                String.join(
                    "\n",
                    "zookeeper.address: localhost:" + zkPort,
                    "bind.listeners: INTERNAL://0.0.0.0:"
                        + tabletInternal
                        + ",CLIENT://0.0.0.0:"
                        + tabletClient,
                    "advertised.listeners: INTERNAL://localhost:"
                        + tabletInternal
                        + ",CLIENT://localhost:"
                        + tabletClient,
                    "internal.listener.name: INTERNAL",
                    "tablet-server.id: 1",
                    "data.dir: /tmp/fluss/data",
                    "remote.data.dir: /tmp/fluss/remote-data",
                    "default.replication.factor: 1"))
            .dependsOn(coordinator);

    FlussTestCluster cluster =
        new FlussTestCluster(
            "localhost:" + coordinatorClient, List.of(tablet, coordinator, zookeeper));
    try {
      tablet.start();
      requireReachable(cluster.bootstrap);
      awaitServing(cluster.bootstrap);
    } catch (RuntimeException e) {
      cluster.close();
      throw e;
    }
    return cluster;
  }

  private static GenericContainer<?> flussServer(String role, int clientPort, String properties) {
    return new GenericContainer<>(DockerImageName.parse(FLUSS_IMAGE))
        .withCommand(role)
        .withNetworkMode("host")
        .withEnv("FLUSS_PROPERTIES", properties)
        .waitingFor(new LocalPortWait(clientPort))
        .withStartupTimeout(STARTUP);
  }

  /** Waits until a host-network container listens on {@code localhost:port}. */
  private static final class LocalPortWait extends AbstractWaitStrategy {
    private final int port;

    LocalPortWait(int port) {
      this.port = port;
    }

    @Override
    protected void waitUntilReady() {
      try {
        Unreliables.retryUntilTrue(
            (int) startupTimeout.getSeconds(),
            TimeUnit.SECONDS,
            () -> {
              if (!waitStrategyTarget.isRunning()) {
                throw new IllegalStateException(
                    "container exited before localhost:" + port + " was listening");
              }
              return listening(port);
            });
      } catch (TimeoutException e) {
        throw new IllegalStateException(
            "localhost:" + port + " was not listening within " + startupTimeout, e);
      }
    }
  }

  private static boolean listening(int port) {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress("localhost", port), 1000);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new IllegalStateException("could not reserve a free host port for Fluss", e);
    }
  }

  private static void requireReachable(String hostPort) {
    String[] hp = hostPort.split(",")[0].split(":");
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])), 5000);
    } catch (IOException | RuntimeException e) {
      throw new IllegalStateException(
          "Fluss is not reachable at "
              + hostPort
              + ". Set "
              + BOOTSTRAP_ENV
              + " to a running cluster (podman compose -f docker-compose-fluss.yml up -d) or leave"
              + " it unset so the test starts one through Testcontainers: "
              + e,
          e);
    }
  }

  /**
   * The tablet server accepts connections before it has registered with the coordinator and before
   * the buckets of a new table have a leader; a client in that window sees "Alive tablet server is
   * empty" or a failed write. The cluster counts as started once a probe table can be created,
   * written and read back.
   */
  private static void awaitServing(String bootstrap) {
    Configuration conf = new Configuration();
    conf.set(ConfigOptions.BOOTSTRAP_SERVERS, Arrays.asList(bootstrap.split(",")));
    long deadline = System.nanoTime() + STARTUP.toNanos();
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try (Connection conn = ConnectionFactory.createConnection(conf)) {
        if (probeRoundTrip(conn)) {
          return;
        }
      } catch (Exception e) {
        last = e;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while waiting for Fluss", e);
      }
    }
    throw new IllegalStateException(
        "Fluss at " + bootstrap + " did not serve a probe table within " + STARTUP, last);
  }

  private static boolean probeRoundTrip(Connection conn) throws Exception {
    String database = "agentic_it_probe";
    TablePath path =
        TablePath.of(database, "ready_" + UUID.randomUUID().toString().replace("-", ""));
    try (Admin admin = conn.getAdmin()) {
      if (!admin.databaseExists(database).get()) {
        admin.createDatabase(database, DatabaseDescriptor.EMPTY, true).get();
      }
      Schema schema =
          Schema.newBuilder()
              .column("key", DataTypes.STRING())
              .column("payload", DataTypes.STRING())
              .primaryKey("key")
              .build();
      admin
          .createTable(
              path, TableDescriptor.builder().schema(schema).distributedBy(1, "key").build(), true)
          .get();
    }
    String value = UUID.randomUUID().toString();
    Table table = conn.getTable(path);
    UpsertWriter writer = table.newUpsert().createWriter();
    GenericRow row = new GenericRow(2);
    row.setField(0, BinaryString.fromString("ready"));
    row.setField(1, BinaryString.fromString(value));
    writer.upsert(row).get();
    writer.flush();
    GenericRow key = new GenericRow(1);
    key.setField(0, BinaryString.fromString("ready"));
    InternalRow read = table.newLookup().createLookuper().lookup(key).get().getSingletonRow();
    return read != null && value.equals(read.getString(1).toString());
  }

  private static void requireContainerRuntime() {
    try {
      DockerClientFactory.instance().client();
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Integration tests need a container runtime for Testcontainers but none is reachable."
              + " With Podman: export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock and"
              + " TESTCONTAINERS_RYUK_DISABLED=true, or set "
              + BOOTSTRAP_ENV
              + " to an already running Fluss cluster. Cause: "
              + e,
          e);
    }
  }

  @Override
  public void close() {
    for (GenericContainer<?> container : containers) {
      container.stop();
    }
  }
}
