package org.agentic.flink.typeinfo;
import org.apache.flink.api.common.functions.OpenContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.a2a.bridge.InProcA2ABridge;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.core.AgentExecutionState;
import org.agentic.flink.example.banking.BankingAgentSetup;
import org.agentic.flink.example.banking.TurnBrain;
import org.agentic.flink.example.banking.graph.BankingAgentGraph;
import org.agentic.flink.example.banking.graph.BankingPath;
import org.agentic.flink.llm.ChatMessage;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The teeth: runs the hot paths on a MiniCluster with {@code disableGenericTypes()}, so Flink
 * <b>throws</b> if any stream element or keyed-state type falls back to Kryo. Passing = the
 * pipelines are provably Kryo-free for the framework's value types.
 */
final class KryoDisabledPipelineTest {

  private JobClient job;

  @AfterEach
  void tearDown() throws Exception {
    if (job != null) {
      job.cancel().get();
      job = null;
    }
    InProcA2ABridge.Hub.reset();
  }

  private static StreamExecutionEnvironment strictEnv() {
    // Flink 2.x: ExecutionConfig.disableGenericTypes() is gone; the Kryo-fallback guard is now a
    // pipeline option set on the cluster Configuration.
    Configuration conf = new Configuration();
    conf.set(PipelineOptions.GENERIC_TYPES, false); // any Kryo fallback now throws at job build/submit
    return StreamExecutionEnvironment.createLocalEnvironment(1, conf);
  }

  @Test
  @DisplayName("banking graph runs with generic types disabled (A2A*, BankingTurn, RoutingBudget state are Kryo-free)")
  void bankingGraphKryoFree() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("req-" + UUID.randomUUID(), "resp-" + UUID.randomUUID());
    try (A2AGatewayConnector connector = bridge.openGateway()) {
      StreamExecutionEnvironment env = strictEnv();
      BankingAgentGraph.wire(
          env,
          bridge,
          BankingAgentSetup.Role.CS,
          (BankingPath p) -> (TurnBrain) (userText, ctx) -> "ok:" + p.name(),
          null,
          4,
          12,
          60_000L);
      job = env.executeAsync("kryo-free-banking");
      Thread.sleep(300);

      A2ARequest t =
          new A2ARequest(
              UUID.randomUUID().toString(),
              "ctx-kryo",
              "agent",
              A2AMessage.userText(UUID.randomUUID().toString(), "what cards do you offer?"),
              false,
              null,
              null);
      connector.publishRequest(t);
      A2AResponse r = connector.awaitFinal(t.getTaskId(), 20_000);
      assertNotNull(r, "graph must run under disableGenericTypes (no Kryo)");
      assertEquals(A2ATaskState.COMPLETED, r.getState());
    }
  }

  @Test
  @DisplayName("AgentEvent + AgentContext/AgentExecutionState/ChatMessage state run with generic types disabled")
  void agentEventStateKryoFree() throws Exception {
    StreamExecutionEnvironment env = strictEnv();
    AgentEvent ev = new AgentEvent();
    ev.setFlowId("flow-kryo");
    ev.setEventType(AgentEventType.FLOW_STARTED);
    env.fromElements(ev)
        .keyBy((KeySelector<AgentEvent, String>) AgentEvent::getFlowId)
        .process(new StateTouchingFn())
        .returns(AgentEvent.class)
        .addSink(new DiscardingSink<>());
    // execute() builds serializers for the stream element (AgentEvent) and all three state types;
    // with generic types disabled, a Kryo fallback on any of them throws here.
    env.execute("kryo-free-agentevent");
  }

  /** Forces serializers for AgentContext / AgentExecutionState / ChatMessage keyed state. */
  static final class StateTouchingFn extends KeyedProcessFunction<String, AgentEvent, AgentEvent> {
    private static final long serialVersionUID = 1L;
    private transient ValueState<AgentContext> ctxState;
    private transient ValueState<AgentExecutionState> execState;
    private transient ListState<ChatMessage> transcript;

    @Override
    public void open(OpenContext openContext) {
      ctxState = getRuntimeContext().getState(new ValueStateDescriptor<>("ctx", AgentContext.class));
      execState =
          getRuntimeContext().getState(new ValueStateDescriptor<>("exec", AgentExecutionState.class));
      transcript =
          getRuntimeContext().getListState(new ListStateDescriptor<>("transcript", ChatMessage.class));
    }

    @Override
    public void processElement(AgentEvent event, Context ctx, Collector<AgentEvent> out)
        throws Exception {
      AgentContext c = new AgentContext();
      c.setFlowId(event.getFlowId());
      ctxState.update(c);
      AgentExecutionState s = new AgentExecutionState();
      s.setFlowId(event.getFlowId());
      execState.update(s);
      transcript.add(ChatMessage.user("hello"));
      out.collect(event);
    }
  }
}
