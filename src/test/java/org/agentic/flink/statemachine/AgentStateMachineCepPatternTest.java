package org.agentic.flink.statemachine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

/** The CEP patterns the legacy DSL job graph is built from must be accepted by Flink CEP. */
class AgentStateMachineCepPatternTest {

  private static AgentStateMachine stateMachine() {
    AgentStateMachine.Builder b =
        AgentStateMachine.builder()
            .withId("sm-" + UUID.randomUUID())
            .withInitialState(AgentState.INITIALIZED);
    b.addTransition(t(AgentState.INITIALIZED, AgentState.EXECUTING, AgentEventType.FLOW_STARTED));
    b.addTransition(t(AgentState.EXECUTING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(
        t(AgentState.VALIDATING, AgentState.COMPLETED, AgentEventType.VALIDATION_PASSED));
    b.addTransition(
        t(AgentState.CORRECTING, AgentState.COMPLETED, AgentEventType.CORRECTION_COMPLETED));
    b.addTransition(
        t(AgentState.SUPERVISOR_REVIEW, AgentState.COMPLETED, AgentEventType.SUPERVISOR_APPROVED));
    b.addTransition(t(AgentState.PAUSED, AgentState.COMPLETED, AgentEventType.FLOW_RESUMED));
    b.addTransition(t(AgentState.OFFLOADING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(
        t(AgentState.COMPENSATING, AgentState.COMPENSATED, AgentEventType.COMPENSATION_COMPLETED));
    return b.build();
  }

  private static AgentTransition t(AgentState from, AgentState to, AgentEventType on) {
    return AgentTransition.builder().from(from).to(to).on(on).build();
  }

  @Test
  void defaultPatternCompiles() {
    Pattern<AgentEvent, ?> pattern = stateMachine().generateCepPattern();
    assertNotNull(NFACompiler.compileFactory(pattern, false).createNFA());
  }

  @Test
  void customPatternsCompile() {
    AgentStateMachine sm = stateMachine();
    for (int mask = 0; mask < 8; mask++) {
      Pattern<AgentEvent, ?> pattern =
          sm.generateCustomCepPattern((mask & 1) != 0, (mask & 2) != 0, (mask & 4) != 0);
      assertNotNull(NFACompiler.compileFactory(pattern, false).createNFA(), "mask " + mask);
    }
  }

  @Test
  void defaultPatternMatchesStartIterateComplete() throws Exception {
    String turn = "turn-" + UUID.randomUUID();
    List<String> matched =
        matches(
            events(
                turn,
                AgentEventType.FLOW_STARTED,
                AgentEventType.LOOP_ITERATION_STARTED,
                AgentEventType.FLOW_COMPLETED));
    assertEquals(List.of(turn), matched, "start, one iteration, completed is one match");
  }

  @Test
  void defaultPatternNeedsAnExecutionEvent() throws Exception {
    List<String> matched =
        matches(
            events(
                "turn-" + UUID.randomUUID(),
                AgentEventType.FLOW_STARTED,
                AgentEventType.FLOW_COMPLETED));
    assertEquals(List.of(), matched);
  }

  private static List<AgentEvent> events(String turn, AgentEventType... types) {
    List<AgentEvent> events = new ArrayList<>();
    String flow = "flow-" + UUID.randomUUID();
    long ts = System.currentTimeMillis();
    for (AgentEventType type : types) {
      AgentEvent e = new AgentEvent(flow, "user", "agent", type);
      e.setTimestamp(++ts);
      e.putData("turn_id", turn);
      events.add(e);
    }
    return events;
  }

  private static List<String> matches(List<AgentEvent> events) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    List<String> matched = new ArrayList<>();
    CEP.pattern(
            env.fromData(events)
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<AgentEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((e, t) -> e.getTimestamp()))
                .keyBy(AgentEvent::getFlowId),
            stateMachine().generateCepPattern())
        .process(
            new PatternProcessFunction<AgentEvent, String>() {
              @Override
              public void processMatch(
                  Map<String, List<AgentEvent>> match, Context ctx, Collector<String> out) {
                out.collect(String.valueOf(match.get("initial").get(0).getData("turn_id")));
              }
            })
        .executeAndCollect()
        .forEachRemaining(matched::add);
    return matched;
  }
}
