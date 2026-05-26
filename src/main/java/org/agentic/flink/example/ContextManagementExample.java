package org.agentic.flink.example;

import org.agentic.flink.context.compaction.*;
import org.agentic.flink.context.core.*;
import org.agentic.flink.context.inverse.InverseRagFunction;
import org.agentic.flink.context.inverse.InverseRagResult;
import org.agentic.flink.context.manager.ContextWindowManager;
import org.agentic.flink.context.memory.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example demonstrating context window management with MoSCoW prioritization
 *
 * <p>Demonstrates: 1. Context window with size limits 2. MoSCoW priority system (MUST, SHOULD,
 * COULD, WONT) 3. Automatic compaction when limits exceeded 4. Relevancy-based pruning 5. Inverse
 * RAG (storing to long-term memory) 6. Memory hierarchy (short-term, long-term, steering)
 */
public class ContextManagementExample {

  public static void main(String[] args) throws Exception {

    // Setup Flink environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration config = new Configuration();
    config.set(PipelineOptions.GENERIC_TYPES, false);
    config.set(PipelineOptions.AUTO_GENERATE_UIDS, false);
    env.configure(config);
    env.setParallelism(1);

    // Step 1: Create sample compaction requests with overflowing context
    DataStream<CompactionRequest> compactionRequests = createSampleCompactionRequests(env);

    // Step 2: Process compaction requests
    DataStream<CompactionResult> compactionResults =
        compactionRequests
            .process(new ContextCompactionFunction(0.5, 0.7))
            .uid(ContextCompactionFunction.UID)
            .name("context-compaction");

    // Step 3: Store promoted items to long-term memory (Inverse RAG)
    DataStream<InverseRagResult> inverseRagResults =
        compactionResults
            .process(new InverseRagFunction())
            .uid(InverseRagFunction.UID)
            .name("inverse-rag-storage");

    // Step 4: Print results
    compactionResults.print().name("compaction-results");
    inverseRagResults.print().name("inverse-rag-results");

    // Execute
    env.execute("Context Management Example");
  }

  private static DataStream<CompactionRequest> createSampleCompactionRequests(
      StreamExecutionEnvironment env) {

    // Create sample contexts that exceed limits
    CompactionRequest request1 = createOverflowedContext("agent-001", "user-001", "flow-001");
    CompactionRequest request2 = createDiversePriorityContext("agent-002", "user-002", "flow-002");

    return env.fromElements(request1, request2).name("compaction-requests");
  }

  /**
   * Create a context that has exceeded token limits Demonstrates compaction due to size
   */
  private static CompactionRequest createOverflowedContext(
      String agentId, String userId, String flowId) {

    // Small context window for demo (normally 4000 tokens)
    AgentContext context = new AgentContext(agentId, flowId, userId, 500, 20);
    context.setCurrentIntent("Understand Apache Flink state management");

    // Add MUST items (always keep)
    context.addContext(
        createItem(
            "Apache Flink provides stateful stream processing with exactly-once guarantees. "
                + "State can be keyed or operator state.",
            ContextPriority.MUST,
            "state-management"));

    context.addContext(
        createItem(
            "Flink supports multiple state backends: RocksDB, HashMapStateBackend, and EmbeddedRocksDB.",
            ContextPriority.MUST,
            "state-management"));

    // Add SHOULD items (important, but can compress)
    context.addContext(
        createItem(
            "RocksDB state backend is recommended for large state and provides incremental checkpointing.",
            ContextPriority.SHOULD,
            "state-management"));

    context.addContext(
        createItem(
            "State TTL can be configured to automatically clean up old state entries.",
            ContextPriority.SHOULD,
            "state-management"));

    context.addContext(
        createItem(
            "Flink uses asynchronous snapshots for checkpointing without stopping processing.",
            ContextPriority.SHOULD,
            "checkpointing"));

    // Add COULD items (nice to have, easily discarded)
    context.addContext(
        createItem(
            "The HashMapStateBackend stores state in memory as Java objects.",
            ContextPriority.COULD,
            "state-backends"));

    context.addContext(
        createItem(
            "State can be queried using the Queryable State feature.",
            ContextPriority.COULD,
            "advanced-features"));

    // Add WONT items (not relevant, discard immediately)
    context.addContext(
        createItem(
            "Apache Kafka is a distributed streaming platform.",
            ContextPriority.WONT,
            "unrelated"));

    context.addContext(
        createItem(
            "Docker containers provide application isolation.",
            ContextPriority.WONT,
            "unrelated"));

    return new CompactionRequest(
        context,
        "Understand Apache Flink state management",
        CompactionRequest.CompactionReason.TOKEN_LIMIT_EXCEEDED);
  }

  /**
   * Create context with diverse priorities Demonstrates MoSCoW-based compaction
   */
  private static CompactionRequest createDiversePriorityContext(
      String agentId, String userId, String flowId) {

    AgentContext context = new AgentContext(agentId, flowId, userId, 400, 15);
    context.setCurrentIntent("Learn about Complex Event Processing in Flink");

    // MUST: Hard facts
    context.addContext(
        createItem(
            "Flink CEP library provides API for detecting complex patterns in event streams.",
            ContextPriority.MUST,
            "cep-basics"));

    context.addContext(
        createItem(
            "Patterns are created using Pattern API with operations like begin(), next(), followedBy().",
            ContextPriority.MUST,
            "cep-basics"));

    // SHOULD: Important context
    context.addContext(
        createItem(
            "CEP supports various quantifiers: oneOrMore(), times(), optional().",
            ContextPriority.SHOULD,
            "cep-quantifiers"));

    context.addContext(
        createItem(
            "Patterns can have time constraints using within() method.",
            ContextPriority.SHOULD,
            "cep-timing"));

    // COULD: Supplementary info
    context.addContext(
        createItem(
            "CEP library uses NFACompiler internally for pattern matching.",
            ContextPriority.COULD,
            "cep-internals"));

    context.addContext(
        createItem(
            "After-match skip strategies control what happens after a match is found.",
            ContextPriority.COULD,
            "cep-advanced"));

    // WONT: Irrelevant
    context.addContext(
        createItem(
            "Python is a popular programming language.",
            ContextPriority.WONT,
            "unrelated"));

    return new CompactionRequest(
        context,
        "Learn about Complex Event Processing in Flink",
        CompactionRequest.CompactionReason.ITEM_COUNT_EXCEEDED);
  }

  private static ContextItem createItem(String content, ContextPriority priority, String intentTag) {
    ContextItem item = new ContextItem(content, priority, MemoryType.SHORT_TERM);
    item.setIntentTag(intentTag);
    item.setRelevancyScore(0.8); // High relevancy
    return item;
  }

  /**
   * Demonstrates memory hierarchy (short-term, long-term, steering) This is shown conceptually -
   * in real app would be integrated with Flink state
   */
  private static void demonstrateMemoryHierarchy() {
    System.out.println("\n=== Memory Hierarchy Demonstration ===\n");

    // Short-term memory: working memory
    ShortTermMemory shortTerm = new ShortTermMemory(50);
    shortTerm.add(
        createItem(
            "Current user question: How does Flink handle state?",
            ContextPriority.SHOULD,
            "current-query"));
    shortTerm.add(
        createItem("Tool execution result: State is managed per key", ContextPriority.SHOULD, "tool-result"));
    System.out.println("Short-term: " + shortTerm);

    // Long-term memory: persistent facts
    LongTermMemory longTerm = new LongTermMemory();
    longTerm.addFact(
        createItem(
            "User preference: Prefers detailed technical explanations",
            ContextPriority.MUST,
            "user-preference"));
    longTerm.addFact(
        createItem(
            "Domain fact: Apache Flink version 1.17.2 in use",
            ContextPriority.MUST,
            "domain-knowledge"));
    System.out.println("Long-term: " + longTerm);

    // Steering state: MoSCoW rules
    SteeringState steering = new SteeringState();
    steering.addMust("must-001", "Always accurate", "must be factually correct");
    steering.addShould(
        "should-001", "Prefer conciseness", "should be concise when possible");
    steering.addCould("could-001", "Include examples", "could include code examples");
    steering.addWont("wont-001", "No speculation", "don't speculate or guess");
    System.out.println("Steering: " + steering);

    System.out.println("\n=== End Memory Hierarchy Demo ===\n");
  }
}
