package org.jagentic.core.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.Principal;
import org.jagentic.core.TurnError;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.GraphBuilder;

/**
 * Whoever could write a runtime's input topic could claim any principal: the runtime believed the
 * {@code principal} on the record. With the gateway's public keys configured it believes one only
 * with the gateway's signature over that principal and that very turn, and anything else is refused
 * with class {@code policy}, nothing appended, and the runtime carries on.
 */
class SignedPrincipalAttackTest {

  private static final Principal ALICE = new Principal("alice", "acme", List.of(), "https://idp");
  private static final Principal OPS = new Principal("ops", "acme", List.of("approver"), "https://idp");

  private static KeyPair keys() throws Exception {
    return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
  }

  private static final KeyPair GATEWAY;
  private static final PrincipalSeal.Signer SIGNER;

  static {
    try {
      GATEWAY = keys();
      SIGNER = new PrincipalSeal.Signer(GATEWAY.getPrivate(), Duration.ofMinutes(5));
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** A workflow whose pay tool needs an approver, and which reads the gateway's keys as a job does. */
  private static LocalRuntime runtime(boolean twoApprovals) {
    Map<String, Object> path = twoApprovals
        ? Map.of("brain", "llm", "prompt", "pay then wire", "tools", List.of("pay", "wire"))
        : Map.of("brain", "rule", "tools", List.of("pay", "wire"), "tool_triggers", Map.of("pay", "pay"));
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("spec_version", "agentic/v1");
    if (twoApprovals) {
      spec.put("llm", Map.of("provider", "stub", "script", List.of(Map.of("tool", "pay", "args", Map.of()),
          Map.of("tool", "wire", "args", Map.of()), Map.of("text", "done"))));
    }
    spec.put("agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "main"),
        "paths", Map.of("main", path), "verifier", Map.of("kind", "none")));
    spec.put("policies", Map.of("approvers", Map.of("roles", List.of("approver")),
        "tool_approval", Map.of("tools", List.of("pay", "wire"))));
    spec.put("tools", List.of(Map.of("id", "pay", "kind", "counting", "effect", "idempotent"),
        Map.of("id", "wire", "kind", "counting", "effect", "idempotent")));
    spec.put(Identity.SPEC_KEY, Map.of("keys", new PrincipalSeal.Verifier(List.of(GATEWAY.getPublic())).pem()));
    GraphBuilder.Built built = GraphBuilder.build(spec, null);
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), null, new ConversationLog.InMemory());
  }

  private static Event turn(String cid, String tid, String text, Principal who) {
    return new Event(cid, tid, who.subject(), text, Map.of(), null, who);
  }

  private static Event signed(Event e) {
    return e.withPrincipalProof(SIGNER.sign(e, Instant.now()));
  }

  private static void refused(TurnResult r, LocalRuntime rt, String cid, int logged) {
    assertEquals(TurnStatus.FAILED, r.status, String.valueOf(r.error));
    assertEquals(TurnError.ErrorClass.POLICY, r.error.errorClass());
    assertEquals(List.of(), r.events, "nothing is appended");
    assertEquals(logged, rt.log().events(cid).size());
  }

  @Test
  void anUnsignedPrincipalIsRefused() {
    LocalRuntime rt = runtime(false);
    refused(rt.submit(turn("c", "t1", "hello", ALICE)), rt, "c", 0);
    refused(rt.submit(Event.turn("c", "t2", "alice", "hello")), rt, "c", 0);
    assertEquals(TurnStatus.COMPLETED, rt.submit(signed(turn("c", "t3", "hello", ALICE))).status,
        "the runtime carries on");
  }

  @Test
  void aPrincipalSignedWithAnotherKeyIsRefused() throws Exception {
    LocalRuntime rt = runtime(false);
    PrincipalSeal.Signer forger = new PrincipalSeal.Signer(keys().getPrivate(), Duration.ofMinutes(5));
    Event e = turn("c", "t1", "hello", ALICE);
    refused(rt.submit(e.withPrincipalProof(forger.sign(e, Instant.now()))), rt, "c", 0);
    refused(rt.submit(e.withPrincipalProof("v1.e30.AAAA")), rt, "c", 0);
  }

  @Test
  void anExpiredPrincipalIsRefused() {
    LocalRuntime rt = runtime(false);
    Event e = turn("c", "t1", "hello", ALICE);
    refused(rt.submit(e.withPrincipalProof(SIGNER.sign(e, Instant.now().minus(Duration.ofHours(1))))), rt, "c", 0);
  }

  @Test
  void aProofReplayedOntoAnotherTurnOrConversationIsRefused() {
    LocalRuntime rt = runtime(false);
    Event first = signed(turn("c", "t1", "hello", ALICE));
    assertEquals(TurnStatus.COMPLETED, rt.submit(first).status);
    int logged = rt.log().events("c").size();
    refused(rt.submit(turn("c", "t2", "hello", ALICE).withPrincipalProof(first.principalProof())), rt, "c", logged);
    refused(rt.submit(turn("d", "t1", "hello", ALICE).withPrincipalProof(first.principalProof())), rt, "d", 0);
    refused(rt.submit(turn("c", "t3", "pay it", ALICE).withPrincipalProof(first.principalProof())), rt, "c", logged);
  }

  @Test
  void aProofForOnePrincipalDoesNotCarryAnother() {
    LocalRuntime rt = runtime(false);
    Event alice = signed(turn("c", "t1", "hello", ALICE));
    Principal mallory = new Principal("mallory", "acme", List.of(), "https://idp");
    refused(rt.submit(turn("c", "t1", "hello", mallory).withPrincipalProof(alice.principalProof())), rt, "c", 0);
    Principal promoted = new Principal("alice", "acme", List.of("approver"), "https://idp");
    refused(rt.submit(turn("c", "t1", "hello", promoted).withPrincipalProof(alice.principalProof())), rt, "c", 0);
  }

  @Test
  void aTurnWhoseTextChangedAfterSigningIsRefused() {
    LocalRuntime rt = runtime(false);
    Event signed = signed(turn("c", "t1", "hello", ALICE));
    refused(rt.submit(turn("c", "t1", "pay it", ALICE).withPrincipalProof(signed.principalProof())), rt, "c", 0);
  }

  @Test
  void aSignalNeedsItsOwnProof() {
    LocalRuntime rt = runtime(false);
    assertEquals(TurnStatus.SUSPENDED, rt.submit(signed(turn("c", "t1", "pay it", ALICE))).status);
    int logged = rt.log().events("c").size();
    Event approval = new Event("c", "t1", "ops", "", Map.of(), Map.of("approved", true), OPS);
    refused(rt.submit(approval), rt, "c", logged);
    assertEquals(TurnStatus.COMPLETED, rt.submit(signed(approval)).status);
  }

  @Test
  void anOldApprovalCannotAnswerALaterQuestion() {
    LocalRuntime rt = runtime(true);
    assertEquals(TurnStatus.SUSPENDED, rt.submit(signed(turn("c", "t1", "pay and wire", ALICE))).status);
    Event approval = signed(new Event("c", "t1", "ops", "", Map.of(), Map.of("approved", true), OPS));
    TurnResult first = rt.submit(approval);
    assertEquals(TurnStatus.SUSPENDED, first.status, "the second call waits for its own approval");
    int logged = rt.log().events("c").size();
    refused(rt.submit(approval), rt, "c", logged);
    assertTrue(rt.log().events("c").stream().noneMatch(e -> e.type().equals("tool_result")
        && "wire".equals(e.payload().get("tool"))), "wire did not run on the old answer");
  }

  @Test
  void aTurnAlreadyOnTheLogIsTheSameTurnWhenItComesAgainLate() {
    LocalRuntime rt = runtime(false);
    Event e = turn("c", "t1", "hello", ALICE);
    Event late = e.withPrincipalProof(SIGNER.sign(e, Instant.now().minus(Duration.ofMinutes(4))));
    assertEquals(TurnStatus.COMPLETED, rt.submit(late).status);
    Event expired = e.withPrincipalProof(SIGNER.sign(e, Instant.now().minus(Duration.ofHours(1))));
    assertEquals(TurnStatus.DUPLICATE, rt.submit(expired).status);
  }
}
