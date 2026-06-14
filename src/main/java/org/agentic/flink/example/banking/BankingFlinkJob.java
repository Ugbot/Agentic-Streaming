package org.agentic.flink.example.banking;

import org.agentic.flink.a2a.bridge.A2ABridge;
import org.agentic.flink.a2a.bridge.A2ABridgeFactory;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.example.banking.graph.BankingAgentGraph;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Flink-side process of a banking agent container: boots an embedded local Flink MiniCluster,
 * wires the role's {@link BankingAgentGraph} to the A2A {@link A2ABridge} (Redis by default), and
 * runs it. The Quarkus gateway (a separate process in the same container) publishes each inbound
 * {@code message/send} onto the bridge and awaits the verifier's response — so the gateway speaks
 * A2A while the routed graph does the work, joined by Redis.
 *
 * <p>Both agent containers share one Redis, so the bridge channels are <b>namespaced by role</b>
 * ({@code a2a:personal:req|resp} vs {@code a2a:cs:req|resp}) to keep the two agents' traffic apart;
 * the gateway derives the same names from its role. Env (see below) overrides every default.
 *
 * <pre>
 *   A2A_BANKING_ROLE=personal|cs        (role; default personal)
 *   AGENTIC_FLINK_A2A_BRIDGE_TRANSPORT  (default redis)
 *   AGENTIC_FLINK_REDIS_HOST/PORT       (default redis:6379 in compose / localhost)
 *   conversation.store=redis            (so the cross-process transcript/phase live in Redis)
 *   CS_AGENT_URL                        (personal only; spec message/send client to the CS agent)
 *   A2A_FLINK_PARALLELISM               (default 1 — a single MiniCluster instance)
 * </pre>
 */
public final class BankingFlinkJob {

  private static final Logger LOG = LoggerFactory.getLogger(BankingFlinkJob.class);

  private BankingFlinkJob() {}

  public static void main(String[] args) throws Exception {
    String roleEnv = env("A2A_BANKING_ROLE", "personal");
    BankingAgentSetup.Role role =
        "cs".equalsIgnoreCase(roleEnv) ? BankingAgentSetup.Role.CS : BankingAgentSetup.Role.PERSONAL;
    String roleName = role == BankingAgentSetup.Role.CS ? "cs" : "personal";

    // Default the bridge to Redis with role-namespaced channels (env still wins). The gateway in
    // this same container derives the identical names from its role, so the two halves rendezvous.
    defaultSysProp("agentic.flink." + ConfigKeys.A2A_BRIDGE_TRANSPORT, "redis");
    defaultSysProp("agentic.flink." + ConfigKeys.A2A_BRIDGE_REQUEST_ENDPOINT, "a2a:" + roleName + ":req");
    defaultSysProp("agentic.flink." + ConfigKeys.A2A_BRIDGE_RESPONSE_ENDPOINT, "a2a:" + roleName + ":resp");

    AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();

    // Personal→CS round-trip must speak spec message/send (the CS gateway only accepts that), so use
    // the hand-rolled HTTP client, not the SDK one. CS role has no peer.
    BankingTurnContext.CustomerServiceClient cs =
        role == BankingAgentSetup.Role.PERSONAL
            ? BankingA2AServer.httpCsClient(env("CS_AGENT_URL", null))
            : null;
    BankingAgentSetup setup = BankingAgentSetup.fromEnv(role, cs);

    int parallelism = intEnv("A2A_FLINK_PARALLELISM", 1);
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(parallelism, new Configuration());

    A2ABridge bridge = A2ABridgeFactory.create(config);
    LOG.info(
        "Banking Flink job [{}] starting: bridge transport={} req={} resp={}",
        roleName,
        bridge.transport(),
        config.get(ConfigKeys.A2A_BRIDGE_REQUEST_ENDPOINT),
        config.get(ConfigKeys.A2A_BRIDGE_RESPONSE_ENDPOINT));

    BankingAgentGraph.wire(env, bridge, setup);
    env.execute("banking-" + roleName); // blocks; keeps the MiniCluster alive for the container
  }

  private static void defaultSysProp(String key, String value) {
    // Only set when neither the env var nor an explicit system property already provides it.
    String envKey = "AGENTIC_FLINK_" + key.replace("agentic.flink.", "").replace('.', '_').toUpperCase();
    if (System.getenv(envKey) == null && System.getProperty(key) == null) {
      System.setProperty(key, value);
    }
  }

  private static String env(String key, String def) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? def : v;
  }

  private static int intEnv(String key, int def) {
    String v = System.getenv(key);
    try {
      return v == null || v.isBlank() ? def : Integer.parseInt(v.trim());
    } catch (NumberFormatException e) {
      return def;
    }
  }
}
