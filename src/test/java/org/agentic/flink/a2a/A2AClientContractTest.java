package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** Exercises the {@link A2AClient} default await/stream contract via {@link FakeA2AClient}. */
class A2AClientContractTest {

  private final Random random = new Random();

  private RemoteAgentSpec spec() {
    return RemoteAgentSpec.builder()
        .withName("peer-" + UUID.randomUUID())
        .withEndpointUrl("https://peer/a2a")
        .withPollInterval(java.time.Duration.ofMillis(1))
        .withRequestTimeout(java.time.Duration.ofSeconds(5))
        .build();
  }

  @RepeatedTest(15)
  @DisplayName("sendAndAwait polls until a completed terminal state and returns the artifact")
  void sendAndAwaitCompletes() {
    int workingPolls = random.nextInt(5); // 0..4 poll cycles before completion
    RemoteAgentSpec spec = spec();
    try (A2AClient client = new FakeA2AClient(spec, workingPolls, false)) {
      String text = "compute-" + random.nextInt();
      A2ATask result =
          client.sendAndAwait(
              A2AMessage.userText(UUID.randomUUID().toString(), text),
              spec.pollIntervalMs(),
              spec.requestTimeoutMs());
      assertEquals(A2ATaskState.COMPLETED, result.getState());
      assertEquals(1, result.getArtifacts().size());
      assertTrue(result.getArtifacts().get(0).textContent().contains(text));
    }
  }

  @Test
  @DisplayName("sendAndAwait surfaces a failed terminal state")
  void sendAndAwaitFails() {
    RemoteAgentSpec spec = spec();
    try (A2AClient client = new FakeA2AClient(spec, 2, true)) {
      A2ATask result =
          client.sendAndAwait(
              A2AMessage.userText(UUID.randomUUID().toString(), "x"),
              spec.pollIntervalMs(),
              spec.requestTimeoutMs());
      assertEquals(A2ATaskState.FAILED, result.getState());
    }
  }

  @Test
  @DisplayName("sendAndAwait throws on timeout when the task never finalizes")
  void sendAndAwaitTimesOut() {
    RemoteAgentSpec spec = spec();
    // workingPolls huge -> never completes within the short timeout.
    try (A2AClient client = new FakeA2AClient(spec, Integer.MAX_VALUE, false)) {
      assertThrows(
          A2AClientException.class,
          () ->
              client.sendAndAwait(
                  A2AMessage.userText(UUID.randomUUID().toString(), "x"), 1, 50));
    }
  }

  @Test
  @DisplayName("stream default delivers a single final update")
  void streamDefaultDelivers() {
    RemoteAgentSpec spec = spec();
    try (A2AClient client = new FakeA2AClient(spec, 1, false)) {
      List<A2ATask> updates = new ArrayList<>();
      A2ATask last =
          client.stream(A2AMessage.userText(UUID.randomUUID().toString(), "hi"), updates::add);
      assertEquals(1, updates.size());
      assertTrue(last.getState().isFinal());
    }
  }

  @Test
  @DisplayName("fetchCard returns the peer card")
  void fetchCard() {
    RemoteAgentSpec spec = spec();
    try (A2AClient client = new FakeA2AClient(spec, 0, false)) {
      A2AAgentCard card = client.fetchCard();
      assertEquals(spec.name(), card.getName());
      assertTrue(card.skill("echo").isPresent());
    }
  }
}
