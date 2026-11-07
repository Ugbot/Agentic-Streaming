package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** OPA can narrow what a document allows and never widen it, and anything but a clear yes is a no. */
class OpaToolPolicyTest {

  private static final ToolPolicy.Request REQUEST = new ToolPolicy.Request("pay", ToolOrigin.BRAIN, "main",
      "non_idempotent", Map.of("amount", 5), Principal.of("alice", "acme"));

  private HttpServer server;
  private final AtomicReference<String> answer = new AtomicReference<>("{\"result\":true}");
  private final AtomicReference<String> seen = new AtomicReference<>();
  private final AtomicInteger asked = new AtomicInteger();
  private volatile int status = 200;

  @BeforeEach
  void start() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", x -> {
      asked.incrementAndGet();
      seen.set(new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      byte[] body = answer.get().getBytes(StandardCharsets.UTF_8);
      x.sendResponseHeaders(status, body.length);
      try (OutputStream out = x.getResponseBody()) {
        out.write(body);
      }
    });
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private OpaToolPolicy opa() {
    return new OpaToolPolicy("http://127.0.0.1:" + server.getAddress().getPort(), "agentic/tools", Duration.ofSeconds(2));
  }

  @Test
  void aClearYesIsAllowedAndSeesWhoIsAsking() {
    assertTrue(opa().decide(REQUEST).allowed());
    assertTrue(seen.get().contains("\"subject\":\"alice\""), seen.get());
    assertTrue(seen.get().contains("\"amount\":5"), seen.get());
    answer.set("{\"result\":{\"allow\":true}}");
    assertTrue(opa().decide(REQUEST).allowed());
  }

  @ParameterizedTest
  @ValueSource(strings = {"{\"result\":false}", "{\"result\":{\"allow\":false}}", "{}", "{\"result\":null}",
      "{\"result\":{\"allow\":\"yes\"}}", "{\"result\":\"true\"}", "{\"result\":1}", "not json", "[]"})
  void anythingElseIsANo(String body) {
    answer.set(body);
    assertFalse(opa().decide(REQUEST).allowed());
  }

  @Test
  void aReasonIsPassedOn() {
    answer.set("{\"result\":{\"allow\":false,\"reason\":\"not after hours\"}}");
    assertEquals("not after hours", opa().decide(REQUEST).reason());
  }

  @Test
  void anErrorStatusIsANo() {
    status = 500;
    assertFalse(opa().decide(REQUEST).allowed());
  }

  @Test
  void anOpaThatIsDownIsANo() {
    assertFalse(new OpaToolPolicy("http://127.0.0.1:9", "p", Duration.ofMillis(300)).decide(REQUEST).allowed());
  }

  @Test
  void itCannotWidenTheDocumentAndARefusalThereNeverLeavesTheProcess() {
    ToolPolicy document = ToolPolicy.of(Map.of("main", Set.of("look")), Set.of());
    ToolPolicy both = document.and(opa());
    assertFalse(both.decide(REQUEST).allowed());
    assertEquals(0, asked.get());
    assertTrue(both.outsideThisProcess());
  }

  @Test
  void itNarrowsTheDocument() {
    answer.set("{\"result\":false}");
    ToolPolicy document = ToolPolicy.of(Map.of("main", Set.of("pay")), Set.of());
    assertFalse(document.and(opa()).decide(REQUEST).allowed());
    assertEquals(1, asked.get());
  }

  /** A policy that changed its mind must not turn a recorded call into a different turn. */
  @Test
  void aCallAlreadyBegunIsNotAskedAboutAgain() {
    AgentContext ctx = new AgentContext("c", "t", "u", new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), new ToolRegistry().registerKeyed("pay", "", null, ToolEffect.IDEMPOTENT,
            (a, c) -> "paid"), null, new ConversationLog.InMemory(), Policies.DEFAULTS);
    ctx.journal = new EffectJournal.InMemory();
    ctx.policy = ToolPolicy.of(Map.of("main", Set.of("pay")), Set.of()).and(opa());
    ctx.path = "main";
    assertEquals("paid", ctx.callTool("pay", Map.of()));
    answer.set("{\"result\":false}");
    assertEquals(1, asked.get());

    AgentContext rerun = new AgentContext("c", "t", "u", new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), ctx.tools, null, ctx.log, Policies.DEFAULTS);
    rerun.journal = ctx.journal;
    rerun.policy = ctx.policy;
    rerun.path = "main";
    assertEquals("paid", rerun.callTool("pay", Map.of()));
    assertEquals(1, asked.get(), "the recorded call is reused, and OPA is not asked again");
    assertEquals(List.of("pay"), rerun.toolCalls);
  }
}
