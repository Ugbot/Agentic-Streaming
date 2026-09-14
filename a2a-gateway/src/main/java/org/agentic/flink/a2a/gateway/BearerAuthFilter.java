package org.agentic.flink.a2a.gateway;

import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.util.Set;

/**
 * Rejects every request that does not carry a valid shared bearer token with HTTP 401. Only the
 * public Agent Card is exempt. The authenticated {@link GatewayAuth.Principal} is stored on the
 * request under {@link GatewayAuth#REQUEST_PROPERTY} for resources that need the caller identity.
 */
@Provider
@Priority(Priorities.AUTHENTICATION)
public class BearerAuthFilter implements ContainerRequestFilter {

  static final Set<String> PUBLIC_PATHS = Set.of(".well-known/agent-card.json", "q/health", "q/health/live", "q/health/ready");

  @Inject GatewayAuth auth;

  @Override
  public void filter(ContainerRequestContext ctx) {
    String path = ctx.getUriInfo().getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    if (PUBLIC_PATHS.contains(path)) {
      return;
    }
    try {
      ctx.setProperty(GatewayAuth.REQUEST_PROPERTY, auth.authenticate(ctx.getHeaders().getFirst("Authorization")));
    } catch (GatewayAuth.Unauthorized e) {
      ctx.abortWith(
          Response.status(Response.Status.UNAUTHORIZED)
              .header("WWW-Authenticate", "Bearer")
              .type(MediaType.APPLICATION_JSON)
              .entity("{\"error\":\"unauthorized\",\"message\":\"" + e.getMessage() + "\"}")
              .build());
    }
  }
}
