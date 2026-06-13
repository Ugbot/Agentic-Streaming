package org.agentic.flink.a2a.gateway.banking;

import io.a2a.server.agentexecution.AgentExecutor;
import io.a2a.server.agentexecution.RequestContext;
import io.a2a.server.tasks.AgentEmitter;
import io.a2a.spec.A2AError;
import io.a2a.spec.Part;
import io.a2a.spec.TextPart;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.agentic.flink.example.banking.BankingAgentSetup;
import org.agentic.flink.example.banking.BankingTurnContext;
import org.agentic.flink.example.banking.safety.RoutingBudget;
import org.agentic.flink.screening.ScreeningResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-process banking {@link AgentExecutor}: runs the full safety stack + bounded ReAct brain inside
 * the gateway JVM (no Flink cluster), so the two A2A agents can be stood up with just an LLM key.
 * Selected when {@code a2a.mode=banking}; the role ({@code personal} | {@code cs}) comes from
 * {@code a2a.banking.role}.
 *
 * <p>Per inbound turn: bind the per-{@code contextId} {@link RoutingBudget}, screen the message
 * ({@code BLOCK} → safe refusal, never reaching the brain), run the brain with a budget-gated
 * context, and return the reply as a Task artifact. The brain is identical to the Flink-operator
 * path ({@code BankingTurnFunction}); only the host differs — the documented "swap" to running
 * inside Flink later.
 */
@ApplicationScoped
@IfBuildProperty(name = "a2a.mode", stringValue = "banking")
public class BankingAgentExecutor implements AgentExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BankingAgentExecutor.class);

  @ConfigProperty(name = "a2a.banking.role", defaultValue = "personal")
  String roleName;

  private volatile BankingAgentSetup setup;
  private final Map<String, RoutingBudget> budgets = new ConcurrentHashMap<>();

  /** Built lazily on first request so the gateway boots without an LLM key (key needed per call). */
  private BankingAgentSetup setup() {
    BankingAgentSetup s = setup;
    if (s == null) {
      synchronized (this) {
        if (setup == null) {
          BankingAgentSetup.Role role =
              "cs".equalsIgnoreCase(roleName)
                  ? BankingAgentSetup.Role.CS
                  : BankingAgentSetup.Role.PERSONAL;
          setup = BankingAgentSetup.fromEnv(role);
          LOG.info("BankingAgentExecutor initialized for role {}", role);
        }
        s = setup;
      }
    }
    return s;
  }

  @Override
  public void execute(RequestContext context, AgentEmitter emitter) throws A2AError {
    String contextId =
        context.getContextId() != null ? context.getContextId() : UUID.randomUUID().toString();
    String userText = textOf(context);
    long now = System.currentTimeMillis();

    BankingAgentSetup setup = setup();
    RoutingBudget budget =
        budgets.computeIfAbsent(
            contextId,
            k ->
                new RoutingBudget(
                    setup.maxRoundTrips(), setup.maxIterations(), setup.turnDeadlineMs(), 8));
    budget.startTurn(now);

    ScreeningResult screen = setup.screening().screen(contextId, userText, now);
    if ("BLOCK".equals(screen.verdict)) {
      LOG.info("Screening BLOCK for ctx {}: {}", contextId, screen.reason);
      reply(emitter, "I'm sorry, I can't help with that request.");
      return;
    }

    final RoutingBudget b = budget;
    String replyText =
        org.agentic.flink.example.banking.env.EnvSession.withContext(
            contextId,
            () -> setup.brain().respond(userText, new BankingTurnContext(contextId, b, now, setup.cs())));
    reply(emitter, replyText);
  }

  @Override
  public void cancel(RequestContext context, AgentEmitter emitter) throws A2AError {
    reply(emitter, "Request canceled.");
  }

  private void reply(AgentEmitter emitter, String text) {
    List<Part<?>> parts = List.of(new TextPart(text == null ? "" : text));
    emitter.addArtifact(parts, "reply", null, null);
    emitter.complete();
  }

  private static String textOf(RequestContext context) {
    if (context.getMessage() == null || context.getMessage().parts() == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (Part<?> p : context.getMessage().parts()) {
      if (p instanceof TextPart) {
        if (sb.length() > 0) {
          sb.append('\n');
        }
        sb.append(((TextPart) p).text());
      }
    }
    return sb.toString();
  }
}
