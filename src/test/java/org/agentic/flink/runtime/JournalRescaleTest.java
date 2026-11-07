package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.runtime.testkit.MiniClusterWorkflowDriver;
import org.agentic.flink.runtime.testkit.TestClusters;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.Test;

/**
 * A rollback that also changes the job's parallelism, on a real job: turns run at 2 subtasks, the
 * job is cancelled, and it restores a savepoint from before those turns at 3 subtasks, so key
 * groups move and every conversation's log forgets the payment. The bank must still see one call
 * each.
 */
class JournalRescaleTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int MAX_PARALLELISM = 128;
  private static final int CONVERSATIONS = 12;

  @Test
  void aRescaleOverTheProcessJournalPaysOnce() throws Exception {
    restoreAtAnotherParallelism(
        "{\"kind\": \"memory\", \"name\": \"%s\", \"x-process-local\": true}"
            .formatted(UUID.randomUUID()));
  }

  static void restoreAtAnotherParallelism(String journal) throws Exception {
    List<String> keys = Collections.synchronizedList(new ArrayList<>());
    HttpServer bank = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    bank.createContext(
        "/pay",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          keys.add(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
          byte[] body = "{\"paid\":true}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    bank.start();
    MiniCluster cluster = TestClusters.start(8);
    Path savepoints = Files.createTempDirectory("journal-rescale");
    try {
      Map<String, Object> workflow = workflow(bank.getAddress().getPort(), journal);
      List<String> conversations = new ArrayList<>();
      for (int i = 0; i < CONVERSATIONS; i++) {
        conversations.add("c-" + i + "-" + UUID.randomUUID());
      }
      assertTrue(
          conversations.stream().anyMatch(JournalRescaleTest::moves), "no key moved subtask");
      try (MiniClusterWorkflowDriver driver =
          new MiniClusterWorkflowDriver(
              cluster, workflow, FlinkRuntimeOptions.fromSpec(workflow), savepoints, 2)) {
        driver.start();
        List<Event> hello = new ArrayList<>();
        List<Event> pay = new ArrayList<>();
        for (String c : conversations) {
          hello.add(Event.turn(c, "t0", "u", "hello"));
          pay.add(Event.turn(c, "t1", "u", "pay"));
        }
        driver.submitAll(hello);
        String beforeThePayments = driver.savepoint();
        for (TurnResult r : driver.submitAll(pay)) {
          assertEquals(TurnStatus.COMPLETED, r.status, r.toString());
        }
        assertEquals(CONVERSATIONS, keys.size());

        driver.rollBackTo(beforeThePayments, 3);

        for (TurnResult r : driver.submitAll(pay)) {
          assertEquals(TurnStatus.COMPLETED, r.status, "reused from the journal: " + r);
          assertEquals(1, r.calls.size(), r.toString());
        }
      }
      assertEquals(CONVERSATIONS, keys.size(), "the bank saw " + keys);
      assertEquals(CONVERSATIONS, keys.stream().distinct().count());
    } finally {
      cluster.close();
      bank.stop(0);
    }
  }

  private static boolean moves(String key) {
    int group = KeyGroupRangeAssignment.assignToKeyGroup(key, MAX_PARALLELISM);
    return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, group)
        != KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 3, group);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> workflow(int port, String journal) throws Exception {
    return JSON.readValue(
        """
        {"spec_version": "agentic/v1",
         "agent": {"id": "a", "router": {"kind": "keyword", "default": "p", "rules": {}},
                   "paths": {"p": {"brain": "rule", "tools": ["pay"], "tool_triggers": {"pay": "pay"}}}},
         "tools": [{"id": "pay", "kind": "http", "effect": "non_idempotent",
                    "url": "http://127.0.0.1:%d/pay"}],
         "stores": {"effects": %s}}
        """
            .formatted(port, journal),
        Map.class);
  }
}
