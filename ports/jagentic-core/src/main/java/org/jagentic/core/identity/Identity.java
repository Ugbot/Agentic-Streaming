package org.jagentic.core.identity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * How a runtime knows who a turn is from, as its operator configured it.
 *
 * <ul>
 *   <li><b>Signed</b>: {@code AGENTIC_PRINCIPAL_KEYS} names a PEM file of the gateway's Ed25519
 *       public keys (or the system property {@code agentic.principal_keys}). Every turn and signal
 *       must carry a {@code principal_proof} the gateway signed for it, and one that does not is
 *       refused with class {@code policy} and nothing appended.
 *   <li><b>Unauthenticated</b>: {@code AGENTIC_IDENTITY=unauthenticated}, for a deployment with no
 *       gateway. The principal on a record is believed as written, so whoever can write the input
 *       topic can claim any principal, and the runtime says so in its log when it starts.
 * </ul>
 *
 * <p>A runtime embedded in a program with neither believes the principals that program builds, as
 * before. A Kafka front door will not start with neither: it must be told which it is.
 */
public final class Identity {

  /** The workflow key a front door puts its verifier under, so the job carries the keys with it. */
  public static final String SPEC_KEY = "x-agentic-identity";

  private Identity() {}

  /** The verifier this process is configured with, or null. */
  public static PrincipalSeal.Verifier fromEnvironment() {
    return fromSettings(key -> {
      String set = System.getProperty("agentic." + key.substring("AGENTIC_".length()).toLowerCase(java.util.Locale.ROOT));
      return set != null ? set : System.getenv(key);
    });
  }

  static PrincipalSeal.Verifier fromSettings(UnaryOperator<String> settings) {
    String file = settings.apply("AGENTIC_PRINCIPAL_KEYS");
    if (file == null || file.isBlank()) {
      return null;
    }
    try {
      return PrincipalSeal.Verifier.fromPem(Files.readString(Path.of(file.strip()), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new IllegalStateException("AGENTIC_PRINCIPAL_KEYS cannot be read: " + file, e);
    }
  }

  /** The verifier a front door put into {@code spec}, or null. */
  public static PrincipalSeal.Verifier fromSpec(Map<String, Object> spec) {
    if (spec != null && spec.get(SPEC_KEY) instanceof Map<?, ?> block && block.get("keys") instanceof String pem) {
      return PrincipalSeal.Verifier.fromPem(pem);
    }
    return null;
  }

  /** What the graph checks principals with: the process's keys, else those the job carries, else none. */
  public static PrincipalSeal.Verifier forGraph(Map<String, Object> spec) {
    PrincipalSeal.Verifier here = fromEnvironment();
    return here != null ? here : fromSpec(spec);
  }

  /**
   * For a front door that reads a topic: the spec to run, carrying the keys so that every worker
   * checks principals with them, or the spec as it is in the unauthenticated mode, which this logs.
   * Throws when the operator chose neither.
   *
   * @param topic the input topic, named in the warning
   */
  public static Map<String, Object> forTopic(Map<String, Object> spec, Map<String, String> env, String topic) {
    PrincipalSeal.Verifier keys = forFrontDoor(env, topic);
    if (keys == null) {
      return spec;
    }
    Map<String, Object> out = new LinkedHashMap<>(spec);
    out.put(SPEC_KEY, Map.of("keys", keys.pem()));
    return out;
  }

  /**
   * For a front door that reads a topic: the gateway's keys, or null in the unauthenticated mode,
   * which this logs. Throws when the operator chose neither.
   */
  public static PrincipalSeal.Verifier forFrontDoor(Map<String, String> env, String topic) {
    PrincipalSeal.Verifier keys = fromSettings(env::get);
    if (keys != null) {
      return keys;
    }
    if ("unauthenticated".equalsIgnoreCase(String.valueOf(env.get("AGENTIC_IDENTITY")).strip())) {
      System.err.println("WARNING: identity is unauthenticated. AGENTIC_IDENTITY=unauthenticated: the principal"
          + " on each record of " + topic + " is believed as written, so anything that can write " + topic
          + " can claim any principal. Restrict who may write it with Kafka ACLs, or put the gateway in"
          + " front and set AGENTIC_PRINCIPAL_KEYS.");
      return null;
    }
    throw new IllegalStateException("say who may claim a principal on " + topic + ": set AGENTIC_PRINCIPAL_KEYS"
        + " to the gateway's public keys, or AGENTIC_IDENTITY=unauthenticated for a deployment with no gateway");
  }
}
