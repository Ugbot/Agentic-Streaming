package org.agentic.flink.job;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.tool.ToolRegistry;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;

/**
 * The legacy single-agent job graph builds from a default {@link Agent} (no custom state machine)
 * and contains the compensation operator exactly when the agent enables compensation.
 */
class AgentJobGeneratorGraphTest {

  private static StreamGraph streamGraph(Agent agent) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
    AgentJob job =
        AgentJob.builder()
            .withId("job-" + UUID.randomUUID())
            .withAgent(agent)
            .withToolRegistry(ToolRegistry.empty())
            .build();
    DataStream<AgentEvent> input =
        env.fromData(new AgentEvent("f", "u", agent.getAgentId(), AgentEventType.FLOW_STARTED));
    AgentJobGenerator.create(env, job)
        .generate(input)
        .addSink(new org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink<>())
        .name("sink");
    return env.getStreamGraph();
  }

  private static List<String> operatorNames(Agent agent) {
    return streamGraph(agent).getStreamNodes().stream().map(StreamNode::getOperatorName).toList();
  }

  private static StreamNode node(StreamGraph graph, String name) {
    return graph.getStreamNodes().stream()
        .filter(n -> name.equals(n.getOperatorName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no operator named " + name));
  }

  /** True when records leaving {@code from} can arrive at {@code to}, following the graph's edges. */
  private static boolean feeds(StreamGraph graph, StreamNode from, StreamNode to) {
    Set<Integer> seen = new HashSet<>();
    Deque<StreamNode> todo = new ArrayDeque<>(List.of(from));
    while (!todo.isEmpty()) {
      for (StreamEdge edge : todo.pop().getOutEdges()) {
        if (edge.getTargetId() == to.getId()) {
          return true;
        }
        if (seen.add(edge.getTargetId())) {
          todo.push(graph.getStreamNode(edge.getTargetId()));
        }
      }
    }
    return false;
  }

  @Test
  void defaultAgentBuildsAGraphWithoutACompensationOperator() {
    String id = "agent-" + UUID.randomUUID();
    Agent agent = Agent.builder().withId(id).withSystemPrompt("help").build();
    List<String> names = operatorNames(agent);
    assertTrue(names.contains("dispatch-" + id), names.toString());
    assertTrue(names.contains("execute-" + id), names.toString());
    assertTrue(names.contains("route-" + id), names.toString());
    assertFalse(names.contains("compensate-" + id), names.toString());
  }

  @Test
  void compensatingAgentWiresTheCompensationOperatorBetweenExecutionAndRouting() {
    String id = "agent-" + UUID.randomUUID();
    Agent agent =
        Agent.builder()
            .withId(id)
            .withSystemPrompt("help")
            .withTools("charge")
            .withCompensatingTool("charge", "refund")
            .build();
    // Stream node ids come from a JVM-wide counter and StreamGraph keeps them in a HashMap, so the
    // iteration order of getStreamNodes() depends on how many transformations ran before this
    // test. Only the edges carry the wiring. Sweep the counter so the assertion is seen to hold at
    // every offset, not only the lucky ones.
    for (int offset = 0; offset < 64; offset++) {
      Transformation.getNewNodeId();
      StreamGraph graph = streamGraph(agent);
      StreamNode execute = node(graph, "execute-" + id);
      StreamNode compensate = node(graph, "compensate-" + id);
      StreamNode route = node(graph, "route-" + id);
      assertTrue(feeds(graph, execute, compensate), "execute must feed compensate");
      assertTrue(feeds(graph, compensate, route), "compensate must feed route");
      assertFalse(feeds(graph, route, compensate), "route must not feed compensate");
      assertFalse(feeds(graph, compensate, execute), "compensate must not feed execute");
    }
  }
}
