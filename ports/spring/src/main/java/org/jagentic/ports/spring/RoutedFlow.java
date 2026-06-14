package org.jagentic.ports.spring;

import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;

import org.jagentic.core.AgentContext;
import org.jagentic.core.Banking;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.TurnResult;

/**
 * The routed graph as a Spring Integration EIP topology (design doc §3e / §4).
 *
 * <p>The core {@link RoutedGraph}'s {@code router -> path -> verifier} maps onto EIP
 * almost 1:1:
 * <ul>
 *   <li><b>Content-Based Router</b> ({@code .route(...)}) = {@link Banking#router} —
 *       classifies each {@link Event} to a path key ({@code cards|payments|general}).</li>
 *   <li><b>Channels</b> ({@code path.cards}, {@code path.payments}, {@code path.general})
 *       = the paths; each is fed by a {@code channelMapping}.</li>
 *   <li><b>Service activators</b> on each path channel = the path brains; they hand the
 *       turn to the shared {@link RoutedGraph} and forward to the verify channel.</li>
 *   <li><b>Verifier</b> (a service activator on {@code verify.in}) = the rule-based
 *       {@code RoutedGraph.Verifier}, here surfaced as the final endpoint.</li>
 * </ul>
 *
 * <p>This intentionally <em>shows the EIP wiring</em> (router -> per-path channels ->
 * verify) the doc describes. The actual turn is delegated to the pure-core
 * {@link RoutedGraph} so the routing/verification logic stays single-sourced and the
 * Spring layer is purely the transport topology. Keying by {@code conversationId} on
 * the channels (and, in a Kafka deployment, on the bindings) preserves per-conversation
 * ordering — the EIP analog of Flink's "all operators keyed by contextId."
 */
@Configuration
public class RoutedFlow {

  /** A request as it flows through the integration channels. */
  public record TurnRequest(Event event, AgentContext ctx) {}

  // --- Channels: the entry point, the three per-path channels, and the verify sink. ---

  @Bean
  public MessageChannel requestsIn() {
    return MessageChannels.direct().getObject();
  }

  @Bean
  public MessageChannel pathCards() {
    return MessageChannels.direct().getObject();
  }

  @Bean
  public MessageChannel pathPayments() {
    return MessageChannels.direct().getObject();
  }

  @Bean
  public MessageChannel pathGeneral() {
    return MessageChannels.direct().getObject();
  }

  @Bean
  public MessageChannel verifyIn() {
    return MessageChannels.direct().getObject();
  }

  /**
   * The Content-Based Router: classify the inbound turn and dispatch to the matching
   * per-path channel. Same classification as the core {@link Banking#router}.
   */
  @Bean
  public IntegrationFlow routerFlow() {
    return IntegrationFlow.from(requestsIn())
        .<TurnRequest, String>route(
            tr -> Banking.router(tr.event(), tr.ctx()),
            mapping -> mapping
                .channelMapping("cards", "pathCards")
                .channelMapping("payments", "pathPayments")
                .channelMapping("general", "pathGeneral"))
        .get();
  }

  /** Cards path: service activator -> verify channel. */
  @Bean
  public IntegrationFlow cardsPath(RoutedGraph graph) {
    return IntegrationFlow.from(pathCards())
        .<TurnRequest, TurnResult>transform(tr -> graph.handle(tr.event(), tr.ctx()))
        .channel(verifyIn())
        .get();
  }

  /** Payments path: service activator -> verify channel. */
  @Bean
  public IntegrationFlow paymentsPath(RoutedGraph graph) {
    return IntegrationFlow.from(pathPayments())
        .<TurnRequest, TurnResult>transform(tr -> graph.handle(tr.event(), tr.ctx()))
        .channel(verifyIn())
        .get();
  }

  /** General path: service activator -> verify channel. */
  @Bean
  public IntegrationFlow generalPath(RoutedGraph graph) {
    return IntegrationFlow.from(pathGeneral())
        .<TurnRequest, TurnResult>transform(tr -> graph.handle(tr.event(), tr.ctx()))
        .channel(verifyIn())
        .get();
  }

  /**
   * The verifier endpoint: the core graph already ran its rule-based verifier inside
   * {@code handle(...)}, so this final service activator validates the {@code ok} flag
   * and surfaces the reply (the EIP analog of an aggregator/filter at the tail of the
   * flow).
   */
  @Bean
  public IntegrationFlow verifyFlow() {
    return IntegrationFlow.from(verifyIn())
        .handle(TurnResult.class, (result, headers) -> {
          if (!result.ok) {
            return new TurnResult(result.conversationId,
                "[verifier] rejected: " + result.reply, List.of());
          }
          return result;
        })
        .get();
  }

  /**
   * Convenience factory mirroring the controller's per-turn context construction, so
   * callers driving the flow programmatically build the same {@link AgentContext}.
   */
  public static TurnRequest turnRequest(Event event,
                                        ConversationStore store,
                                        KeyedStateStore state,
                                        ToolRegistry tools,
                                        Retrieval.TwoTierRetriever retriever) {
    AgentContext ctx = new AgentContext(
        event.conversationId(), event.userId(), store, state, tools, retriever);
    return new TurnRequest(event, ctx);
  }
}
