package org.jagentic.pekko.http;

import java.time.Duration;

import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoSystem;

/** Boots the Pekko system + HTTP front door over the banking graph. Run with
 * {@code mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.http.HttpMain}.
 * Then: {@code curl localhost:8080/.well-known/agent-card.json} and
 * {@code curl -XPOST localhost:8080/agent -d '{"conversation_id":"c1","user_id":"u","text":"what is my balance?"}'}. */
public final class HttpMain {

  private HttpMain() {}

  public static void main(String[] args) {
    String host = env("AGENTIC_PEKKO_HTTP_HOST", "0.0.0.0");
    int port = Integer.parseInt(env("AGENTIC_PEKKO_HTTP_PORT", "8080"));
    PekkoSystem sys = new PekkoSystem(AgentDeps.banking());
    HttpFrontDoor.start(sys.system(), host, port,
        AgentCard.defaultCard("http://" + host + ":" + port), Duration.ofSeconds(20));
    System.out.println("agentic-pekko HTTP front door on " + host + ":" + port);
  }

  private static String env(String key, String fallback) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? fallback : v;
  }
}
