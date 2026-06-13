package org.agentic.flink.a2a.bridge;

import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;

/**
 * Builds an {@link A2ABridge} from configuration, selecting the transport via {@code
 * a2a.bridge.transport}.
 *
 * <ul>
 *   <li>{@code inproc} — {@link InProcA2ABridge} (embedded gateway / tests).
 *   <li>{@code zeromq} — {@link ZeroMqA2ABridge} (default; localhost / single host, no broker).
 *   <li>{@code redis} — {@link RedisA2ABridge} (distributed-light; needs Redis + the optional Jedis
 *       dep). Request/response endpoints are used as Redis pub/sub channel names; host/port come
 *       from {@code redis.host}/{@code redis.port}.
 * </ul>
 *
 * <p>{@code kafka} is recognized as a transport name but not yet wired here; use {@code zeromq} or
 * {@code redis} for the distributed path (the request side can alternatively be fed by the existing
 * {@code KafkaChannel} directly).
 */
public final class A2ABridgeFactory {

  private A2ABridgeFactory() {}

  /** Build a bridge from an {@link AgenticFlinkConfig} (env/system/explicit resolution). */
  public static A2ABridge create(AgenticFlinkConfig config) {
    String transport =
        config.get(ConfigKeys.A2A_BRIDGE_TRANSPORT, ConfigKeys.DEFAULT_A2A_BRIDGE_TRANSPORT);
    String request =
        config.get(
            ConfigKeys.A2A_BRIDGE_REQUEST_ENDPOINT, ConfigKeys.DEFAULT_A2A_BRIDGE_REQUEST_ENDPOINT);
    String response =
        config.get(
            ConfigKeys.A2A_BRIDGE_RESPONSE_ENDPOINT,
            ConfigKeys.DEFAULT_A2A_BRIDGE_RESPONSE_ENDPOINT);

    switch (transport.toLowerCase()) {
      case "inproc":
        return new InProcA2ABridge(request, response);
      case "zeromq":
        return new ZeroMqA2ABridge(request, response);
      case "redis":
        String host = config.get(ConfigKeys.REDIS_HOST, ConfigKeys.DEFAULT_REDIS_HOST);
        int port =
            config.getInt(
                ConfigKeys.REDIS_PORT, Integer.parseInt(ConfigKeys.DEFAULT_REDIS_PORT));
        return new RedisA2ABridge(host, port, request, response);
      case "kafka":
        throw new IllegalArgumentException(
            "Kafka A2A bridge transport is not yet wired. Use 'zeromq' or 'redis', or feed the "
                + "request side via the existing KafkaChannel directly.");
      default:
        throw new IllegalArgumentException(
            "Unknown a2a.bridge.transport: " + transport + ". Supported: inproc, zeromq, redis.");
    }
  }

  /** Build a bridge directly by transport + endpoints (no config object). */
  public static A2ABridge create(String transport, String requestEndpoint, String responseEndpoint) {
    switch (transport.toLowerCase()) {
      case "inproc":
        return new InProcA2ABridge(requestEndpoint, responseEndpoint);
      case "zeromq":
        return new ZeroMqA2ABridge(requestEndpoint, responseEndpoint);
      default:
        throw new IllegalArgumentException(
            "create(transport, req, resp) supports inproc/zeromq; use create(config) for redis.");
    }
  }
}
