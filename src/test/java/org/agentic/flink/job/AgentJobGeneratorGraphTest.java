package org.agentic.flink.job;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.tool.ToolRegistry;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;

/**
 * The legacy single-agent job graph builds from a default {@link Agent} (no custom state machine)
 * and contains the compensation operator exactly when the agent enables compensation.
 */
class AgentJobGeneratorGraphTest {

  private static List<String> operatorNames(Agent agent) {
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
    return env.getStreamGraph().getStreamNodes().stream().map(StreamNode::getOperatorName).toList();
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
    List<String> names = operatorNames(agent);
    int execute = names.indexOf("execute-" + id);
    int compensate = names.indexOf("compensate-" + id);
    int route = names.indexOf("route-" + id);
    assertTrue(execute >= 0 && compensate > execute && route > compensate, names.toString());
  }
}
