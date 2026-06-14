package org.agentic.flink.plan;
import org.apache.flink.api.common.functions.OpenContext;

import org.agentic.flink.python.PythonAction;
import org.agentic.flink.python.PythonExecutor;
import org.agentic.flink.python.PythonToolExecutor;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator that runs an {@link AgentPlan}.
 *
 * <p>{@code open()} resolves Java SPIs (chat connection, embedder, corpus, …) via reflection,
 * binds every Python tool/action/listener to a per-slot {@link PythonExecutor}, and registers
 * Java + Python tools in a {@link ToolRegistry}. {@code processElement()} dispatches the
 * incoming event to every matching {@link PythonAction} and emits whatever the Python callable
 * returns; events with no matching action are passed through unchanged so the operator behaves
 * sanely for plans that only declare tools.
 *
 * <p>The chat connection itself is bound lazily on first reference — declaring a chat connection
 * in the plan should not force a network handshake at job start-up. Phase 3 leaves the chat path
 * accessible to Python actions via the {@code ctx} dict; Phase 4+ will expand this into a richer
 * runner-context with corpus/embedder access.
 */
public class AgentPlanProcessFunction<K> extends KeyedProcessFunction<K, Object, Object> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentPlanProcessFunction.class);

  private final String planJson;

  private transient AgentPlan plan;
  private transient PythonExecutor python;
  private transient ToolRegistry toolRegistry;
  private transient List<PythonAction> pythonActions;
  private transient Map<String, Object> resourceInstances;
  private transient Object chatConnectionInstance;

  public AgentPlanProcessFunction(AgentPlan plan) {
    if (plan == null) {
      throw new IllegalArgumentException("AgentPlan must not be null");
    }
    this.planJson = plan.toJson();
  }

  public AgentPlanProcessFunction(String planJson) {
    if (planJson == null || planJson.isEmpty()) {
      throw new IllegalArgumentException("planJson must be non-empty");
    }
    this.planJson = planJson;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    this.plan = AgentPlan.fromJson(planJson);
    this.pythonActions = new ArrayList<>();
    this.resourceInstances = new HashMap<>();

    boolean needsPython =
        !plan.getActions().isEmpty()
            || plan.getTools().stream().anyMatch(ToolSpec::isPython)
            || plan.getListeners().stream().anyMatch(ListenerSpec::isPython);

    if (needsPython) {
      this.python = new PythonExecutor();
      python.open();
    }

    // Resolve named Java resources.
    for (Map.Entry<String, ResourceSpec> e : plan.getResources().entrySet()) {
      resourceInstances.put(e.getKey(), PlanReader.instantiate(e.getValue()));
    }

    // Chat connection (lazy bind on first reference).
    if (plan.getChatConnection() != null) {
      chatConnectionInstance = PlanReader.instantiate(plan.getChatConnection());
    }

    // Build ToolRegistry: java-kind via FQN, python-kind via PythonToolExecutor.
    ToolRegistry.ToolRegistryBuilder regBuilder = ToolRegistry.builder();
    for (ToolSpec t : plan.getTools()) {
      if (t.isJava()) {
        Object inst = PlanReader.instantiate(new ResourceSpec(t.getFqn(), t.getConfig()));
        if (!(inst instanceof ToolExecutor)) {
          throw new IllegalStateException(
              "Java tool '"
                  + t.getName()
                  + "' fqn="
                  + t.getFqn()
                  + " does not implement ToolExecutor");
        }
        regBuilder.registerTool(t.getName(), t.getDescription(), (ToolExecutor) inst);
      } else {
        PythonToolExecutor px =
            new PythonToolExecutor(
                t.getName(), t.getDescription(), t.getCloudpickleB64(), t.getParamNames());
        px.bind(python);
        regBuilder.registerTool(t.getName(), t.getDescription(), px);
      }
    }
    this.toolRegistry = regBuilder.build();

    // Bind Python actions.
    for (ActionSpec a : plan.getActions()) {
      PythonAction pa = new PythonAction(a.getName(), a.getEvents(), a.getCloudpickleB64());
      pa.bind(python);
      pythonActions.add(pa);
    }

    LOG.info(
        "AgentPlanProcessFunction opened: agent_id={}, tools={}, actions={}, resources={},"
            + " python={}",
        plan.getAgentId(),
        plan.getTools().size(),
        plan.getActions().size(),
        plan.getResources().size(),
        python != null);
  }

  @Override
  public void processElement(Object value, Context ctx, Collector<Object> out) throws Exception {
    String eventType = inferEventType(value);
    boolean dispatched = false;
    for (PythonAction action : pythonActions) {
      if (!action.matches(eventType)) {
        continue;
      }
      Map<String, Object> runnerCtx = buildRunnerContext(ctx, eventType);
      Object result = action.invoke(value, runnerCtx);
      if (result != null) {
        out.collect(result);
      }
      dispatched = true;
    }
    if (!dispatched) {
      out.collect(value);
    }
  }

  @Override
  public void close() throws Exception {
    try {
      if (python != null) {
        python.close();
      }
    } finally {
      super.close();
    }
  }

  /** Routing key for an event. Maps map's {@code "type"} key first, otherwise the class name. */
  static String inferEventType(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Map<?, ?> m) {
      Object t = m.get("type");
      if (t != null) {
        return t.toString();
      }
    }
    return value.getClass().getSimpleName();
  }

  private Map<String, Object> buildRunnerContext(Context ctx, String eventType) {
    Map<String, Object> c = new HashMap<>();
    c.put("agent_id", plan.getAgentId());
    c.put("event_type", eventType);
    c.put("key", ctx.getCurrentKey());
    c.put("processing_time", ctx.timerService().currentProcessingTime());
    if (chatConnectionInstance != null) {
      c.put("chat_connection_fqn", plan.getChatConnection().getFqn());
    }
    return c;
  }

  public AgentPlan getPlan() {
    if (plan == null) {
      plan = AgentPlan.fromJson(planJson);
    }
    return plan;
  }

  public String getPlanJson() {
    return planJson;
  }

  public ToolRegistry getToolRegistry() {
    return toolRegistry;
  }
}
