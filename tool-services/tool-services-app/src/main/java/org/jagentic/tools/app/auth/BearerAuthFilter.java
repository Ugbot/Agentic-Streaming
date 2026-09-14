package org.jagentic.tools.app.auth;

import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

/** Applies {@link BearerTokenAuth} to every JAX-RS endpoint (REST {@code /tools} and MCP {@code /mcp}). */
@Provider
@Priority(Priorities.AUTHENTICATION)
public class BearerAuthFilter implements ContainerRequestFilter {

  @Inject
  BearerTokenAuth auth;

  @Override
  public void filter(ContainerRequestContext ctx) {
    if (auth.accepts(ctx.getHeaderString("Authorization"))) {
      return;
    }
    ctx.abortWith(Response.status(Response.Status.UNAUTHORIZED)
        .header("WWW-Authenticate", "Bearer")
        .type(MediaType.APPLICATION_JSON)
        .entity("{\"ok\":false,\"error\":\"unauthorized\"}")
        .build());
  }
}
