package org.jagentic.pekko.http;

import java.time.Duration;

import org.jagentic.gateway.Gateway;
import org.jagentic.gateway.GatewayOptions;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoSystem;

/**
 * Boots a Pekko system behind the gateway, over the banking graph. The gateway's own settings decide
 * how it binds and who gets in, and it refuses to start with no authentication configured: see {@code
 * docs/security.md}.
 */
public final class HttpMain {

  private HttpMain() {}

  public static void main(String[] args) throws Exception {
    GatewayOptions options = GatewayOptions.fromEnvironment(System.getenv());
    PekkoSystem sys = new PekkoSystem(AgentDeps.banking());
    Duration ask = Duration.ofSeconds(20);
    String url = "http://" + options.host() + ":" + options.port();
    Gateway gateway = Gateway.start(options, new PekkoBackend(sys.system(), ask), AgentCard.defaultCard(url).toJson());
    System.out.println("agentic-pekko gateway on " + options.host() + ":" + gateway.port());
  }
}
