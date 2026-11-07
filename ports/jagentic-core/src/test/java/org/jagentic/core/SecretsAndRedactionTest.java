package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpServer;

import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.core.pipeline.WorkflowValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A secret is named, not written, and it reaches the wire and nothing else. */
class SecretsAndRedactionTest {

  private static final String VALUE = "tool-secret-" + System.nanoTime();

  private static Map<String, Object> workflow(String url, Object bearer) {
    return Map.of("spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "main"),
            "paths", Map.of("main", Map.of("brain", "rule", "tools", List.of("call"), "tool_triggers", Map.of("go", "call"))),
            "verifier", Map.of("kind", "none")),
        "tools", List.of(Map.of("id", "call", "kind", "http", "effect", "idempotent", "url", url,
            "auth", Map.of("bearer", bearer))));
  }

  @Test
  void aSecretInADocumentIsRefused() {
    assertThrows(WorkflowValidator.WorkflowValidationException.class,
        () -> GraphBuilder.build(workflow("http://127.0.0.1:9/", "my-actual-token"), null));
    assertThrows(WorkflowValidator.WorkflowValidationException.class,
        () -> GraphBuilder.build(workflow("http://127.0.0.1:9/", "env:not a name"), null));
  }

  @Test
  void theSecretReachesTheToolAndNothingThatIsRecorded(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("token");
    Files.writeString(file, VALUE + "\n");
    List<String> seen = new ArrayList<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", x -> {
      seen.add(x.getRequestHeaders().getFirst("Authorization"));
      byte[] body = "{\"ok\":true}".getBytes();
      x.sendResponseHeaders(200, body.length);
      try (OutputStream out = x.getResponseBody()) {
        out.write(body);
      }
    });
    server.start();
    try {
      GraphBuilder.Built built = GraphBuilder.build(
          workflow("http://127.0.0.1:" + server.getAddress().getPort() + "/", "file:" + file), null);
      EffectJournal.InMemory journal = new EffectJournal.InMemory();
      LocalRuntime rt = new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
          built.tools(), null, new ConversationLog.InMemory()).withJournal(journal);
      TurnResult r = rt.submit(Event.turn("c", "t", "u", "go"));
      assertEquals(TurnStatus.COMPLETED, r.status);
      assertEquals(List.of("Bearer " + VALUE), seen);
      assertFalse(rt.log().events("c").toString().contains(VALUE));
      assertFalse(r.toMap().toString().contains(VALUE));
      assertFalse(journal.get(new EffectJournal.Key("c", "t", EffectJournal.TOOL, 0)).orElseThrow().toString().contains(VALUE));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void aSecretThatIsNotThereIsADefiniteFailureThatNamesTheReferenceOnly() {
    Secrets.Unresolved e = assertThrows(Secrets.Unresolved.class, () -> Secrets.resolve("env:AGENTIC_NOT_SET_ANYWHERE"));
    assertTrue(e.getMessage().contains("AGENTIC_NOT_SET_ANYWHERE"));
    assertThrows(Secrets.Unresolved.class, () -> Secrets.resolve("hunter2"));
  }

  @Test
  void aFailingToolsErrorIsRedactedInTheJournalAndOnTheLog() {
    ToolRegistry tools = new ToolRegistry().registerKeyed("boom", "", null, ToolEffect.IDEMPOTENT, (a, c) -> {
      throw new IllegalStateException("rejected Bearer abcdefghijklmnop");
    });
    AgentContext ctx = new AgentContext("c", "t", "u", new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        tools, null, new ConversationLog.InMemory(), Policies.DEFAULTS);
    EffectJournal.InMemory journal = new EffectJournal.InMemory();
    ctx.journal = journal;
    ctx.policy = ToolPolicy.ALLOW_ALL;
    assertThrows(ToolFailure.class, () -> ctx.callTool("boom", Map.of("password", "hunter2")));
    String recorded = ctx.log.events("c").toString() + journal.get(new EffectJournal.Key("c", "t", EffectJournal.TOOL, 0));
    assertFalse(recorded.contains("abcdefghijklmnop"), recorded);
    assertFalse(recorded.contains("hunter2"), recorded);
  }
}
