package org.jagentic.tools.app.auth;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class BearerTokenAuthTest {

  @RepeatedTest(20)
  void acceptsOnlyTheConfiguredToken() {
    String token = UUID.randomUUID().toString();
    BearerTokenAuth auth = new BearerTokenAuth(token, false);
    assertTrue(auth.accepts("Bearer " + token));
    assertTrue(auth.accepts("bearer " + token));
    assertFalse(auth.accepts("Bearer " + UUID.randomUUID()));
    assertFalse(auth.accepts("Basic " + token));
    assertFalse(auth.accepts(token));
    assertFalse(auth.accepts(null));
    assertFalse(auth.accepts("Bearer " + token.substring(0, ThreadLocalRandom.current().nextInt(1, token.length()))));
  }

  @Test
  void missingTokenFailsClosedUnlessDevMode() {
    assertFalse(new BearerTokenAuth(null, false).accepts(null));
    assertFalse(new BearerTokenAuth("", false).accepts("Bearer " + UUID.randomUUID()));
    assertTrue(new BearerTokenAuth(null, true).accepts(null));
    assertTrue(new BearerTokenAuth("  ", true).accepts("Bearer anything"));
  }
}
