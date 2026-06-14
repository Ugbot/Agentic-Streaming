package org.jagentic.ports.temporal;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;

import org.jagentic.ports.temporal.TurnMessages.TurnReply;
import org.jagentic.ports.temporal.TurnMessages.TurnRequest;

/**
 * Runs the banking graph as Temporal entity workflows with <b>no external Temporal
 * server</b>: an in-memory {@link TestWorkflowEnvironment} hosts the worker, exactly as
 * Pekko's {@code LocalDemo} runs actors without a cluster. One workflow per
 * conversationId; {@code c1}'s two turns hit the same durable execution, so its
 * transcript grows across turns — proving Temporal's event-sourced state carries C1.
 *
 * <p>In production you point a {@link WorkflowClient} at a real Temporal service and run
 * a {@link Worker}; the same {@code ConversationWorkflowImpl} runs unchanged.
 */
public final class LocalDemo {

  private static final String TASK_QUEUE = "agentic-banking";

  private LocalDemo() {}

  public static void main(String[] args) {
    TestWorkflowEnvironment env = TestWorkflowEnvironment.newInstance();
    try {
      Worker worker = env.newWorker(TASK_QUEUE);
      worker.registerWorkflowImplementationTypes(ConversationWorkflowImpl.class);
      env.start();

      WorkflowClient client = env.getWorkflowClient();
      Map<String, ConversationWorkflow> stubs = new HashMap<>();

      List<String[]> turns = Arrays.asList(
          new String[] {"c1", "what card types do you offer?"},
          new String[] {"c2", "what is my balance?"},
          new String[] {"c1", "tell me about crypto cash-back"},
          new String[] {"c3", "where is the nearest branch?"});

      System.out.println("=== Agentic-Flink :: Banking RoutedGraph as Temporal entity workflows ===");
      System.out.println("service: in-memory TestWorkflowEnvironment (no external Temporal server)\n");

      Map<String, Integer> turnCounts = new HashMap<>();
      for (String[] t : turns) {
        String cid = t[0];
        String text = t[1];
        ConversationWorkflow wf = stubs.get(cid);
        if (wf == null) {
          // First turn for this conversation: start one durable execution, id == cid.
          WorkflowOptions opts = WorkflowOptions.newBuilder()
              .setTaskQueue(TASK_QUEUE).setWorkflowId(cid).build();
          wf = client.newWorkflowStub(ConversationWorkflow.class, opts);
          WorkflowClient.start(wf::run);
          stubs.put(cid, wf);
        }
        TurnReply reply = wf.turn(new TurnRequest(text, "demo")); // synchronous update
        turnCounts.merge(cid, 1, Integer::sum);
        System.out.printf("[%s] turn=%d path=%s ok=%s reply=%s%n",
            cid, turnCounts.get(cid), reply.path, reply.ok, reply.reply);
      }

      // Prove C1: c1's transcript holds both turns (user+assistant x2), durable in the
      // event-sourced workflow state — read via a Query.
      int c1Count = stubs.get("c1").messageCount();
      System.out.printf("%nc1 durable message count = %d (event-sourced workflow state)%n", c1Count);

      stubs.values().forEach(ConversationWorkflow::close);
    } finally {
      env.close();
    }
  }
}
