package org.agentic.flink.example;

import java.util.UUID;
import org.agentic.flink.a2a.A2AStep;
import org.agentic.flink.a2a.A2ATransport;
import org.agentic.flink.a2a.RemoteAgentSpec;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Demonstrates chaining a remote A2A agent into a Flink workflow, both ways the framework supports.
 *
 * <ol>
 *   <li><b>As a tool</b> — {@code AgentBuilder.withRemoteAgent(...)} exposes a peer to the LLM as
 *       the synthetic tool {@code a2a:<name>} (registered via {@code A2AToolRegistry}).
 *   <li><b>As an explicit step</b> — {@link A2AStep#applyTo} splices a deterministic delegation into
 *       the stream graph, keyed by A2A {@code contextId}.
 * </ol>
 *
 * <p><b>Prerequisites:</b> a reachable A2A peer. Point {@code A2A_PEER_URL} at one — e.g. this
 * project's own gateway (see {@code a2a-gateway/README.md}) or any A2A-compliant agent. Without a
 * peer the explicit step records a per-event error (it is configured with {@code failOnError=false})
 * and the job still completes.
 *
 * <pre>{@code
 * A2A_PEER_URL=http://localhost:9999/a2a \
 *   java -cp target/agentic-flink-1.0.0-SNAPSHOT.jar org.agentic.flink.example.A2AAgentExample
 * }</pre>
 */
public final class A2AAgentExample {

  private A2AAgentExample() {}

  public static void main(String[] args) throws Exception {
    String peerUrl = System.getenv().getOrDefault("A2A_PEER_URL", "http://localhost:9999/a2a");

    // (1) A peer exposed to an agent's LLM as a tool: a2a:planner.
    RemoteAgentSpec planner =
        RemoteAgentSpec.endpoint("planner", peerUrl, A2ATransport.JSONRPC);
    Agent coordinator =
        Agent.builder()
            .withId("coordinator-" + UUID.randomUUID())
            .withSystemPrompt("Coordinate work; delegate planning to the planner peer.")
            .withRemoteAgent(planner)
            .build();
    System.out.println("Agent " + coordinator.getAgentId()
        + " can call remote tools: " + coordinator.getAllowedTools());

    // (2) The same peer as an explicit, deterministic pipeline step.
    A2AStep delegate =
        A2AStep.builder()
            .withName("delegate")
            .withSpec(RemoteAgentSpec.endpoint("planner", peerUrl, A2ATransport.JSONRPC))
            .withInputKey("input")
            .withOutputKey("a2a.result")
            .withFailOnError(false)
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<AgentEvent> input =
        env.fromElements("Plan a trip to SFO", "Plan a trip to JFK")
            .map(A2AAgentExample::toEvent);

    DataStream<AgentEvent> enriched = delegate.applyTo(input);
    enriched
        .map(
            e ->
                "context="
                    + e.getCorrelationId()
                    + " state="
                    + e.getData("a2a.result.state")
                    + " result="
                    + e.getData("a2a.result"))
        .print();

    System.out.println("Running A2A step against peer: " + peerUrl);
    env.execute("a2a-agent-example");
  }

  private static AgentEvent toEvent(String prompt) {
    AgentEvent event =
        new AgentEvent(UUID.randomUUID().toString(), "demo-user", "coordinator",
            AgentEventType.FLOW_STARTED);
    event.setCorrelationId("conv-" + UUID.randomUUID());
    event.putData("input", prompt);
    return event;
  }
}
