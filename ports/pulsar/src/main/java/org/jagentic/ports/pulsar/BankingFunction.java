package org.jagentic.ports.pulsar;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

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
 * The banking {@code router -> path -> verifier} graph hosted as an Apache <b>Pulsar
 * Function</b>. This is the Engine seam realized on Pulsar Functions: the pure-Java
 * core ({@code org.jagentic.core.*}) is reused verbatim; only this binding is new.
 *
 * <p>Mechanical correspondence to Flink:
 * <ul>
 *   <li><b>Flink {@code keyBy(conversationId)}</b> &rarr; the input topic is consumed
 *       with a {@code Key_Shared} subscription and the message <em>key</em> is the
 *       {@code conversationId}, so each conversation is delivered to one function
 *       instance in order — single-writer-per-conversation (<b>C2</b>).</li>
 *   <li><b>Flink checkpointed keyed {@code ValueState}</b> &rarr; the Pulsar
 *       <em>state store</em> ({@code Context.getState/putState}, BookKeeper-backed),
 *       wrapped as {@link PulsarStateConversationStore} + {@link PulsarStateKeyedStore}
 *       — durable keyed state supplied natively by the runtime (<b>C1</b>).</li>
 *   <li><b>Flink job graph</b> &rarr; a deployed function topology (input topic →
 *       function → output topic); chain functions for multi-stage flows (<b>C12</b>).</li>
 *   <li><b>Effectively-once</b> &rarr; Pulsar Functions' processing guarantee + acks
 *       give fault tolerance (<b>C3</b>); the ConversationStore makes a redelivered
 *       turn idempotent.</li>
 * </ul>
 *
 * <p>The input value is the user's text; the {@code conversationId} is the message
 * key (via {@link Record#getKey()}); {@code userId} rides as a message property. The
 * verified reply is returned, which Pulsar publishes to the function's output topic.
 *
 * <p><b>Async-I/O (C4) note.</b> {@code process} runs on the function thread; a real
 * blocking LLM/A2A call here would stall the instance. Pulsar's pattern is to make the
 * call async and publish the reply to a response topic keyed by the same
 * {@code conversationId}, re-entering the function to resume — the analogue of the
 * Kafka Streams response-topic split. The banking graph is rule-based and never
 * blocks, so it runs synchronously inside {@code process()} here.
 */
public final class BankingFunction implements Function<String, String> {

  // Stateless, rebuilt per instance: the graph/tools/retriever are pure functions of
  // the request; all conversation state lives in Pulsar's state store via the context.
  private final RoutedGraph graph;
  private final ToolRegistry tools;
  private final Retrieval.TwoTierRetriever retriever;

  /** Default: the shared banking essence from jagentic-core. Adding a tool/path to the
   * core's {@code Banking} factory propagates here with no change to this class. */
  public BankingFunction() {
    this(Banking.buildGraph(), Banking.defaultTools(), Banking.retriever());
  }

  /** Injectable: run any graph/tools/retriever built from the public core abstractions
   * (e.g. an extended graph with a new path + tool) on this Pulsar Function seam. */
  public BankingFunction(RoutedGraph graph, ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {
    this.graph = graph;
    this.tools = tools;
    this.retriever = retriever;
  }

  /** Adapts a Pulsar {@link Context}'s state API to the narrow {@link StateBytes} seam. */
  static StateBytes stateBytes(Context context) {
    return new StateBytes() {
      @Override
      public byte[] get(String key) {
        ByteBuffer buf = context.getState(key);
        if (buf == null) {
          return null;
        }
        ByteBuffer dup = buf.duplicate();
        byte[] out = new byte[dup.remaining()];
        dup.get(out);
        return out;
      }

      @Override
      public void put(String key, byte[] value) {
        context.putState(key, ByteBuffer.wrap(value));
      }

      @Override
      public void delete(String key) {
        context.deleteState(key);
      }
    };
  }

  @Override
  public String process(String input, Context context) {
    Record<?> record = context.getCurrentRecord();
    String conversationId = record.getKey().orElse("anonymous-conversation");
    Map<String, String> props = record.getProperties();
    String userId = props != null && props.containsKey("userId")
        ? props.get("userId")
        : "user-" + conversationId;

    StateBytes sb = stateBytes(context);
    ConversationStore store = new PulsarStateConversationStore(sb);
    KeyedStateStore keyed = new PulsarStateKeyedStore(sb);

    Event event = new Event(conversationId, userId, input);
    AgentContext agentCtx = new AgentContext(conversationId, userId, store, keyed, tools, retriever);

    // === The engine seam: the portable router->path->verifier graph ===
    TurnResult result = graph.handle(event, agentCtx);
    return result.reply;
  }
}
