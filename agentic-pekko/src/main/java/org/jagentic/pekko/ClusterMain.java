package org.jagentic.pekko;

import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.pekko.cluster.typed.Cluster;

import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.pekko.durability.DurabilityProfile;
import org.jagentic.pekko.http.AgentCard;
import org.jagentic.pekko.http.HttpFrontDoor;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;

/**
 * One node of a clustered agentic-pekko deployment: the banking graph hosted on Cluster Sharding
 * (one event-sourced {@code Conversation} entity per conversation id, placed by the shard region)
 * over the {@link DurabilityProfile} chosen by {@code AGENTIC_PEKKO_DURABILITY} or {@code --profile}.
 * Every turn goes {@link PekkoRuntime} to the sharded router to the entity; nothing is routed
 * through the local {@code ConversationManager}.
 *
 * <pre>
 *   # single-node cluster, in-memory journal, runs the demo turns and exits
 *   ./mvnw -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.ClusterMain
 *
 *   # Postgres journal (application-cluster-jdbc.conf), serve the HTTP front door and stay up
 *   AGENTIC_PEKKO_DURABILITY=postgres AGENTIC_PG_URL=jdbc:postgresql://localhost:5434/agentic \
 *   AGENTIC_PG_USER=agentic AGENTIC_PG_PASSWORD=agentic \
 *   ./mvnw -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.ClusterMain \
 *     -Dexec.args="--http 8080"
 * </pre>
 *
 * Options: {@code --profile memory|postgres|cassandra|redis} (default: the environment variable,
 * else {@code memory}), {@code --port N} (Artery port; {@code 0} = ephemeral; default
 * {@code AGENTIC_PEKKO_PORT}, else the profile's configuration), {@code --host H} (Artery hostname;
 * default {@code AGENTIC_PEKKO_HOST}, else the profile's configuration), {@code --text T} (run one
 * turn instead of the demo script), {@code --http PORT} (serve the HTTP front door on that port and
 * block until the JVM is terminated).
 */
public final class ClusterMain {

  private ClusterMain() {}

  /** Parsed command line; {@code text == null} runs the demo script, {@code httpPort < 0} means exit after the turns. */
  record Options(DurabilityProfile profile, String host, Integer port, String text, int httpPort) {

    static Options parse(String[] args) {
      String profile = System.getenv(DurabilityProfile.ENV_VAR);
      String host = System.getenv("AGENTIC_PEKKO_HOST");
      String port = System.getenv("AGENTIC_PEKKO_PORT");
      String text = null;
      int httpPort = -1;
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--profile" -> profile = value(args, ++i, "--profile");
          case "--host" -> host = value(args, ++i, "--host");
          case "--port" -> port = value(args, ++i, "--port");
          case "--text" -> text = value(args, ++i, "--text");
          case "--http" -> httpPort = Integer.parseInt(value(args, ++i, "--http"));
          default -> throw new IllegalArgumentException("unknown argument '" + args[i]
              + "'; expected --profile, --host, --port, --text or --http");
        }
      }
      return new Options(DurabilityProfile.from(profile),
          host == null || host.isBlank() ? null : host,
          port == null || port.isBlank() ? null : Integer.valueOf(port),
          text, httpPort);
    }

    private static String value(String[] args, int i, String flag) {
      if (i >= args.length) {
        throw new IllegalArgumentException(flag + " needs a value");
      }
      return args[i];
    }

    /** Artery overrides for {@link PekkoSystem#clustered}; empty when neither host nor port was given. */
    Config overrides() {
      StringBuilder sb = new StringBuilder();
      if (host != null) {
        sb.append("pekko.remote.artery.canonical.hostname = \"").append(host).append("\"\n");
      }
      if (port != null) {
        sb.append("pekko.remote.artery.canonical.port = ").append(port).append('\n');
      }
      return ConfigFactory.parseString(sb.toString());
    }
  }

  public static void main(String[] args) throws Exception {
    run(Options.parse(args), System.out, new CountDownLatch(1));
  }

  /**
   * Boots the node, runs the turns and, with {@code --http}, serves until {@code shutdown} counts
   * down (the JVM's shutdown hook does that for {@link #main}). Returns the turn results.
   */
  static List<TurnResult> run(Options opts, PrintStream out, CountDownLatch shutdown) throws Exception {
    AgentDeps deps = AgentDeps.banking();
    try (PekkoSystem sys = PekkoSystem.clustered(deps, opts.profile(), opts.overrides())) {
      Cluster cluster = Cluster.get(sys.system());
      out.printf("agentic-pekko cluster node %s status=%s profile=%s journal=%s sharded=%s%n",
          cluster.selfMember().address(), cluster.selfMember().status(), sys.profile(),
          sys.journalPlugin(), sys.sharded());
      PekkoRuntime rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(10));
      List<Event> turns = opts.text() != null
          ? List.of(new Event("c1", "demo", opts.text()))
          : List.of(
              new Event("c1", "alice", "what card types do you offer?"),
              new Event("c2", "bob", "what is my balance?"),
              new Event("c1", "alice", "tell me about crypto cash-back"),
              new Event("c3", "carol", "hello there"));
      List<TurnResult> results = turns.stream().map(e -> {
        TurnResult r = rt.submit(e);
        out.printf("[%s] path=%-8s ok=%-5s turn=%s reply=%s%n", e.conversationId(), r.path, r.ok, r.turnId, r.reply);
        return r;
      }).toList();
      if (opts.httpPort() >= 0) {
        String host = opts.host() == null ? "0.0.0.0" : opts.host();
        HttpFrontDoor.start(sys.system(), host, opts.httpPort(),
            AgentCard.defaultCard("http://" + host + ":" + opts.httpPort()), Duration.ofSeconds(20));
        out.printf("agentic-pekko HTTP front door on %s:%d (sharded); Ctrl-C to leave the cluster%n",
            host, opts.httpPort());
        Runtime.getRuntime().addShutdownHook(new Thread(shutdown::countDown));
        shutdown.await();
      }
      return results;
    }
  }
}
