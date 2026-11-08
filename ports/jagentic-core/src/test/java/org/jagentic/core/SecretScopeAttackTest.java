package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.jagentic.core.pipeline.GraphBuilder;

/**
 * A document names its secrets, and a document could name any of the process's: {@code env:HOME},
 * {@code file:/etc/passwd}, or {@code ${AWS_SECRET_ACCESS_KEY}} in a URL that a tool then calls. A
 * reference now has to be on the operator's list, {@code AGENTIC_SECRET_REFS}, whose default is
 * {@code env:AGENTIC_*} and files under {@code /run/secrets/}.
 */
class SecretScopeAttackTest {

  @AfterEach
  void clear() {
    System.clearProperty("agentic.secret_refs");
  }

  @Test
  void anEnvironmentVariableOffTheListIsNotRead() {
    assertTrue(System.getenv("PATH") != null);
    Secrets.Unresolved e = assertThrows(Secrets.Unresolved.class, () -> Secrets.resolve("env:PATH"));
    assertTrue(e.getMessage().contains("AGENTIC_SECRET_REFS"), e.getMessage());
  }

  @Test
  void aFileOffTheListIsNotRead() {
    assertThrows(Secrets.Unresolved.class, () -> Secrets.resolve("file:/etc/hosts"));
    assertThrows(Secrets.Unresolved.class, () -> Secrets.resolve("file:/run/secrets/../../etc/hosts"));
  }

  @Test
  void whatTheOperatorListsIsRead(@TempDir Path dir) throws Exception {
    Path token = dir.resolve("token");
    Files.writeString(token, "s3cret\n");
    System.setProperty("agentic.secret_refs", "file:" + dir + "/");
    assertEquals("s3cret", Secrets.resolve("file:" + token));
    assertThrows(Secrets.Unresolved.class, () -> Secrets.resolve("file:" + dir + "/../" + dir.getFileName() + "x"));
  }

  @Test
  void aConnectionLinkOffTheListIsNotExpanded() {
    Map<String, Object> doc = Map.of("spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "main"),
            "paths", Map.of("main", Map.of("brain", "rule", "tools", List.of("call"))),
            "verifier", Map.of("kind", "none")),
        "tools", List.of(Map.of("id", "call", "kind", "http", "effect", "idempotent", "url", "${PATH}")));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> GraphBuilder.build(doc, null));
    assertTrue(e.getMessage().contains("AGENTIC_SECRET_REFS"), e.getMessage());
  }
}
