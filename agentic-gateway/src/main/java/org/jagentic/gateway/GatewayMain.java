package org.jagentic.gateway;

import java.util.Map;

/**
 * Runs the gateway in front of a Flink job between Kafka topics:
 * {@code AGENTIC_GATEWAY_KAFKA_BOOTSTRAP}, {@code AGENTIC_GATEWAY_INPUT_TOPIC} and optionally {@code
 * AGENTIC_GATEWAY_RESULTS_TOPIC}, plus the settings of {@link GatewayOptions}.
 */
public final class GatewayMain {

  private GatewayMain() {}

  public static void main(String[] args) throws Exception {
    Map<String, String> env = System.getenv();
    GatewayOptions options = GatewayOptions.fromEnvironment(env);
    String bootstrap = env.get("AGENTIC_GATEWAY_KAFKA_BOOTSTRAP");
    String input = env.get("AGENTIC_GATEWAY_INPUT_TOPIC");
    if (bootstrap == null || input == null) {
      throw new IllegalArgumentException(
          "set AGENTIC_GATEWAY_KAFKA_BOOTSTRAP and AGENTIC_GATEWAY_INPUT_TOPIC");
    }
    KafkaBackend backend = KafkaBackend.connect(bootstrap, input,
        env.getOrDefault("AGENTIC_GATEWAY_RESULTS_TOPIC", input + ".results"));
    Gateway gateway = Gateway.start(options, backend, Map.of("name", "agentic-gateway"));
    System.out.println("agentic gateway on " + options.host() + ":" + gateway.port());
  }
}
