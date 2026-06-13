package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.job.AgentJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Exercises the explicit {@link A2AStep} pipeline step on a Flink minicluster. */
final class A2AStepTest {

  private static final ConcurrentLinkedQueue<AgentEvent> OUT = new ConcurrentLinkedQueue<>();

  private static A2AStep echoStep() {
    return A2AStep.builder()
        .withName("echo")
        .withSpec(
            RemoteAgentSpec.builder()
                .withName("echo-peer")
                .withEndpointUrl("https://peer/a2a")
                .withPollInterval(Duration.ofMillis(1))
                .build())
        .withClientFactory(s -> new FakeA2AClient(s, 1, false))
        .withOutputKey("a2a.echo")
        .build();
  }

  @Test
  @DisplayName("A2AStep.applyTo enriches each event with the peer's artifact text")
  void enrichesEvents() throws Exception {
    OUT.clear();
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(2, new Configuration());

    List<String> prompts = List.of("plan-a", "plan-b", "plan-c");
    DataStream<AgentEvent> src = env.addSource(new EventSource(prompts));

    echoStep().applyTo(src).addSink(new Collect());
    env.execute("a2a-step-test");

    assertEquals(prompts.size(), OUT.size());
    for (AgentEvent e : OUT) {
      Object text = e.getData("a2a.echo");
      Object state = e.getData("a2a.echo.state");
      assertEquals("completed", state);
      assertTrue(text instanceof String && ((String) text).startsWith("echo: plan-"));
      assertEquals("echo", e.getCurrentStage());
    }
  }

  @Test
  @DisplayName("failOnError=true converts a failed remote task into a FLOW_FAILED event")
  void failOnErrorEmitsFailure() throws Exception {
    OUT.clear();
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());

    A2AStep failing =
        A2AStep.builder()
            .withName("boom")
            .withSpec(
                RemoteAgentSpec.builder()
                    .withName("boom-peer")
                    .withEndpointUrl("https://peer/a2a")
                    .withPollInterval(Duration.ofMillis(1))
                    .build())
            .withClientFactory(s -> new FakeA2AClient(s, 1, true))
            .withFailOnError(true)
            .build();

    DataStream<AgentEvent> src = env.addSource(new EventSource(List.of("x")));
    failing.applyTo(src).addSink(new Collect());
    env.execute("a2a-step-fail-test");

    assertEquals(1, OUT.size());
    assertEquals(AgentEventType.FLOW_FAILED, OUT.peek().getEventType());
  }

  @Test
  @DisplayName("AgentJobBuilder.withA2AStep records steps on the job")
  void jobBuilderRecordsSteps() {
    A2AStep step = echoStep();
    AgentJob job =
        AgentJob.builder()
            .withId("job-" + UUID.randomUUID())
            .withAgent(
                org.agentic.flink.dsl.Agent.builder()
                    .withId("a")
                    .withSystemPrompt("x")
                    .withStateMachine(A2ATestSupport.minimalStateMachine())
                    .build())
            .withA2AStep(step)
            .build();
    assertEquals(1, job.getA2ASteps().size());
    assertEquals("echo", job.getA2ASteps().get(0).name());
  }

  static final class EventSource implements SourceFunction<AgentEvent> {
    private static final long serialVersionUID = 1L;
    private final List<String> prompts;
    private volatile boolean running = true;

    EventSource(List<String> prompts) {
      this.prompts = Collections.unmodifiableList(new ArrayList<>(prompts));
    }

    @Override
    public void run(SourceContext<AgentEvent> ctx) {
      for (String p : prompts) {
        if (!running) {
          return;
        }
        AgentEvent e =
            new AgentEvent(
                UUID.randomUUID().toString(), "user", "local-agent", AgentEventType.FLOW_STARTED);
        e.setCorrelationId("conv-" + p);
        e.putData("result", p);
        ctx.collect(e);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  static final class Collect implements SinkFunction<AgentEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(AgentEvent value, Context context) {
      OUT.add(value);
    }
  }
}
