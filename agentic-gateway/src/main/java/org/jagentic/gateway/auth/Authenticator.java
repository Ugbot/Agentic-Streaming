package org.jagentic.gateway.auth;

import org.jagentic.core.Principal;

/** Turns a bearer token into a principal, or refuses. There is no anonymous answer. */
@FunctionalInterface
public interface Authenticator {
  Principal authenticate(String bearerToken) throws Unauthenticated;
}
