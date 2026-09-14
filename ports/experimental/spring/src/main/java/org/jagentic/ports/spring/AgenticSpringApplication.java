package org.jagentic.ports.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import org.jagentic.core.ConversationStore;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.Banking;

/**
 * Spring Boot entry point for the Agentic-Flink Spring port.
 *
 * <p>This module reuses the pure-Java {@code jagentic-core} byte-for-byte (no Flink
 * dependency). Spring supplies the <em>enterprise wiring</em>: an inbound REST edge
 * ({@link AgentController}) and a Spring Integration EIP topology
 * ({@link RoutedFlow}) that mirrors the core {@link RoutedGraph}'s
 * {@code router -> path -> verifier} shape as channels and endpoints.
 *
 * <p>The singleton core beans below are the Spring analog of Flink's per-key state:
 * a process-local {@link ConversationStore.InMemory} stands in for the durable
 * keyed transcript, and a {@link RoutedGraph} + retriever from {@link Banking}
 * drive each turn. In production the {@code ConversationStore} bean would be swapped
 * for a Redis/JPA-backed implementation behind the same SPI — agent logic unchanged.
 */
@SpringBootApplication
public class AgenticSpringApplication {

  public static void main(String[] args) {
    SpringApplication.run(AgenticSpringApplication.class, args);
  }

  /** Process-wide conversation memory (durable transcript tier; swap for Redis/JPA). */
  @Bean
  public ConversationStore conversationStore() {
    return new ConversationStore.InMemory();
  }

  /** Per-(key,name) scalar slot — the portable form of Flink keyed {@code ValueState}. */
  @Bean
  public KeyedStateStore keyedStateStore() {
    return new KeyedStateStore.InMemory();
  }

  /** The banking tool set (get_balance, ...). */
  @Bean
  public ToolRegistry toolRegistry() {
    return Banking.defaultTools();
  }

  /** Hot-tier retriever seeded with the banking KB (cold tier null). */
  @Bean
  public Retrieval.TwoTierRetriever retriever() {
    return Banking.retriever();
  }

  /** The canonical router -> path -> verifier graph, engine-agnostic and model-free. */
  @Bean
  public RoutedGraph routedGraph() {
    return Banking.buildGraph();
  }
}
