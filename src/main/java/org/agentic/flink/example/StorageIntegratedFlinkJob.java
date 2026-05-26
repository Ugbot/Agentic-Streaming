package org.agentic.flink.example;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.ShortTermMemoryStore;
import org.agentic.flink.storage.config.StorageConfiguration;
import org.agentic.flink.storage.metrics.MetricsWrapper;
import org.agentic.flink.storage.metrics.StorageMetrics;
import java.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Complete example of a Flink job using pluggable multi-tier storage.
 *
 * <p>This example demonstrates:
 *
 * <ul>
 *   <li>Configurable storage backends (in-memory or Redis)
 *   <li>HOT tier (ShortTermMemoryStore) for active context
 *   <li>WARM tier (LongTermMemoryStore) for conversation persistence
 *   <li>Context hydration from WARM storage on startup
 *   <li>Metrics collection and reporting
 *   <li>Multi-user conversation handling
 * </ul>
 *
 * <p>Usage:
 *
 * <pre>{@code
 * # Run with in-memory storage (no dependencies required)
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StorageIntegratedFlinkJob" \
 *   -Dexec.args="memory"
 *
 * # Run with Redis storage (requires Redis running on localhost:6379)
 * docker run -d -p 6379:6379 redis:latest
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StorageIntegratedFlinkJob" \
 *   -Dexec.args="redis"
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public class StorageIntegratedFlinkJob {

  private static final Logger LOG = LoggerFactory.getLogger(StorageIntegratedFlinkJob.class);

  public static void main(String[] args) throws Exception {
    // Parse storage backend from arguments (default: memory)
    String storageBackend = args.length > 0 ? args[0] : "memory";

    LOG.info("Starting Flink job with {} storage backend", storageBackend);

    // Create Flink environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // Single parallelism for demo

    // Create storage configuration
    StorageConfiguration storageConfig = createStorageConfiguration(storageBackend);

    // Create event source (simulated user interactions)
    DataStream<AgentEvent> events =
        env.addSource(new ConversationEventSource())
            .name("ConversationEvents")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<AgentEvent>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        (event, timestamp) ->
                            event.getTimestamp() != null
                                ? event.getTimestamp()
                                : System.currentTimeMillis()));

    // Process events with pluggable storage
    DataStream<String> results =
        events
            .keyBy(event -> event.getData("userId", String.class))
            .process(new StorageAwareConversationProcessor(storageConfig))
            .name("StorageAwareProcessor");

    // Print results
    results.print();

    // Execute
    env.execute("Storage Integrated Flink Job - " + storageBackend);
  }

  /**
   * Creates storage configuration based on backend type.
   *
   * @param backend "memory" or "redis"
   * @return configured StorageConfiguration
   */
  private static StorageConfiguration createStorageConfiguration(String backend) {
    Map<String, String> hotConfig = new HashMap<>();
    Map<String, String> warmConfig = new HashMap<>();

    if ("redis".equalsIgnoreCase(backend)) {
      // Redis HOT tier configuration
      hotConfig.put(ConfigKeys.REDIS_HOST, ConfigKeys.DEFAULT_REDIS_HOST);
      hotConfig.put(ConfigKeys.REDIS_PORT, ConfigKeys.DEFAULT_REDIS_PORT);
      hotConfig.put("redis.database", "0");
      hotConfig.put("redis.ttl.seconds", "3600"); // 1 hour
      hotConfig.put("redis.pool.max.total", "20");
      hotConfig.put("redis.pool.max.idle", "5");

      // Redis WARM tier configuration
      warmConfig.put(ConfigKeys.REDIS_HOST, ConfigKeys.DEFAULT_REDIS_HOST);
      warmConfig.put(ConfigKeys.REDIS_PORT, ConfigKeys.DEFAULT_REDIS_PORT);
      warmConfig.put("redis.database", "1"); // Different database
      warmConfig.put("redis.ttl.seconds", "86400"); // 24 hours
      warmConfig.put("redis.pool.max.total", "20");
      warmConfig.put("redis.pool.max.idle", "5");

      return StorageConfiguration.builder()
          .withHotTier("redis", hotConfig)
          .withWarmTier("redis", warmConfig)
          .build();
    } else {
      // In-memory configuration
      hotConfig.put("cache.max.size", "10000");
      hotConfig.put("cache.ttl.seconds", "3600");

      warmConfig.put("cache.max.size", "5000");
      warmConfig.put("cache.ttl.seconds", "86400");

      return StorageConfiguration.builder()
          .withHotTier("memory", hotConfig)
          .withWarmTier("memory", warmConfig)
          .build();
    }
  }

  /**
   * Keyed process function that uses pluggable storage for conversation management.
   *
   * <p>Features:
   *
   * <ul>
   *   <li>Hydrates conversation context from WARM storage on first access
   *   <li>Maintains active context in HOT storage
   *   <li>Persists conversations to WARM storage periodically
   *   <li>Collects and reports storage metrics
   * </ul>
   */
  private static class StorageAwareConversationProcessor
      extends KeyedProcessFunction<String, AgentEvent, String> {

    private final StorageConfiguration storageConfig;

    // Storage instances (initialized in open())
    private transient ShortTermMemoryStore hotStore;
    private transient LongTermMemoryStore warmStore;
    private transient MetricsWrapper<String, List<ContextItem>> hotMetrics;
    private transient MetricsWrapper<String, AgentContext> warmMetrics;

    // Metrics reporting interval
    private static final long METRICS_INTERVAL_MS = 30000; // 30 seconds
    private long lastMetricsReport = 0;

    public StorageAwareConversationProcessor(StorageConfiguration storageConfig) {
      this.storageConfig = storageConfig;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      super.open(openContext);

      // Initialize storage
      this.hotStore = storageConfig.createShortTermStore();
      this.warmStore = storageConfig.createLongTermStore();

      // Wrap with metrics
      this.hotMetrics = new MetricsWrapper<>(hotStore);
      this.warmMetrics = new MetricsWrapper<>(warmStore);

      this.lastMetricsReport = System.currentTimeMillis();

      LOG.info(
          "Storage initialized: HOT={}, WARM={}",
          hotStore.getClass().getSimpleName(),
          warmStore.getClass().getSimpleName());
    }

    @Override
    public void processElement(AgentEvent event, Context ctx, Collector<String> out)
        throws Exception {

      String userId = event.getData("userId", String.class);
      String flowId = event.getData("flowId", String.class);
      String message = event.getData("message", String.class);

      LOG.debug("Processing event for user={}, flow={}: {}", userId, flowId, message);

      // 1. Try to load context from HOT storage
      Optional<List<ContextItem>> hotContextOpt = hotMetrics.get(flowId);
      List<ContextItem> hotContext =
          hotContextOpt.orElse(new ArrayList<>());

      // 2. If not in HOT, hydrate from WARM storage
      if (hotContext.isEmpty()) {
        LOG.info("Context not in HOT storage, hydrating from WARM for flow={}", flowId);
        Optional<AgentContext> warmContext = warmMetrics.get(flowId);

        if (warmContext.isPresent()) {
          AgentContext context = warmContext.get();
          hotContext = new ArrayList<>(context.getContextWindow().getItems());
          hotMetrics.put(flowId, hotContext);
          LOG.info("Hydrated {} items from WARM to HOT storage", hotContext.size());
        } else {
          // New conversation - initialize
          LOG.info("New conversation started for flow={}", flowId);
          hotContext = new ArrayList<>();
        }
      }

      // 3. Add new context item
      ContextItem newItem =
          new ContextItem(message, ContextPriority.MUST, MemoryType.SHORT_TERM);
      newItem.setItemId("item-" + UUID.randomUUID());
      hotContext.add(newItem);

      // 4. Update HOT storage
      hotMetrics.put(flowId, hotContext);

      // 5. Periodically persist to WARM storage
      if (hotContext.size() % 5 == 0) { // Every 5 messages
        AgentContext agentContext = new AgentContext(
            "conversation-agent",
            flowId,
            userId,
            8000, // max tokens
            50   // max items
        );

        // Add all items to context window
        for (ContextItem item : hotContext) {
          agentContext.addContext(item);
        }

        warmMetrics.put(flowId, agentContext);
        LOG.info("Persisted {} items to WARM storage for flow={}", hotContext.size(), flowId);
      }

      // 6. Generate output
      String result =
          String.format(
              "[%s] User %s: %s (Context size: %d)", flowId, userId, message, hotContext.size());

      out.collect(result);

      // 7. Report metrics periodically
      long currentTime = System.currentTimeMillis();
      if (currentTime - lastMetricsReport >= METRICS_INTERVAL_MS) {
        reportMetrics();
        lastMetricsReport = currentTime;
      }
    }

    /** Reports storage metrics to logs. */
    private void reportMetrics() {
      try {
        StorageMetrics hotStats = hotMetrics.getMetrics();
        StorageMetrics warmStats = warmMetrics.getMetrics();

        LOG.info("=== Storage Metrics Report ===");
        LOG.info("HOT Storage ({}ms latency):", hotStats.getAverageLatencyMs());
        LOG.info(
            "  - Operations: {} ({}% success)",
            hotStats.getTotalOperations(),
            (1.0 - hotStats.getErrorRate()) * 100);
        LOG.info(
            "  - Latency: avg={}ms, max={}ms",
            hotStats.getAverageLatencyMs(),
            hotStats.getMaxGetLatencyMs());
        LOG.info(
            "  - Hit rate: {}%",
            hotStats.getHitRate() * 100);

        LOG.info("WARM Storage ({}ms latency):", warmStats.getAverageLatencyMs());
        LOG.info(
            "  - Operations: {} ({}% success)",
            warmStats.getTotalOperations(),
            (1.0 - warmStats.getErrorRate()) * 100);
        LOG.info(
            "  - Latency: avg={}ms, max={}ms",
            warmStats.getAverageLatencyMs(),
            warmStats.getMaxGetLatencyMs());
        LOG.info("=============================");

      } catch (Exception e) {
        LOG.warn("Failed to report metrics", e);
      }
    }

    @Override
    public void close() throws Exception {
      if (hotStore != null) {
        hotStore.close();
      }
      if (warmStore != null) {
        warmStore.close();
      }
      super.close();
    }
  }

  /**
   * Simulated event source that generates conversation events for multiple users.
   *
   * <p>Generates realistic conversation patterns:
   *
   * <ul>
   *   <li>Multiple users with unique flow IDs
   *   <li>Varying message lengths and topics
   *   <li>Realistic timing between messages
   * </ul>
   */
  private static class ConversationEventSource implements SourceFunction<AgentEvent> {

    private volatile boolean running = true;
    private static final String[] USERS = {"alice", "bob", "charlie", "diana"};
    private static final String[][] CONVERSATIONS = {
      {
        "Hello, I need help with my order",
        "My order number is 12345",
        "Can you check the status?",
        "Thank you for the help!",
        "One more question...",
        "When will it arrive?",
        "Great, thanks!",
        "Goodbye"
      },
      {
        "Hi there",
        "I have a technical question",
        "How do I reset my password?",
        "I tried that already",
        "Still not working",
        "Can you help?",
        "Thanks!",
        "Problem solved"
      },
      {
        "Good morning",
        "I want to upgrade my plan",
        "What are the options?",
        "How much does it cost?",
        "Can I get a discount?",
        "Sounds good",
        "Let's proceed",
        "Thank you"
      },
      {
        "Hey",
        "Quick question",
        "Do you support international shipping?",
        "What about customs fees?",
        "How long does it take?",
        "Perfect",
        "I'll place an order",
        "Thanks for the info"
      }
    };

    @Override
    public void run(SourceContext<AgentEvent> ctx) throws Exception {
      Random random = new Random();
      int messageCount = 0;

      while (running && messageCount < 100) {
        // Select random user
        String user = USERS[random.nextInt(USERS.length)];
        String flowId = "flow-" + user;
        String[] conversation = CONVERSATIONS[random.nextInt(CONVERSATIONS.length)];

        // Select random message from conversation
        String message = conversation[random.nextInt(conversation.length)];

        // Create event
        AgentEvent event =
            new AgentEvent(flowId, user, "conversation-agent", AgentEventType.USER_INPUT_RECEIVED);

        event.putData("userId", user);
        event.putData("flowId", flowId);
        event.putData("message", message);

        ctx.collect(event);

        messageCount++;

        // Sleep between messages (100-500ms)
        Thread.sleep(100 + random.nextInt(400));
      }

      LOG.info("Event source completed - generated {} messages", messageCount);
    }

    @Override
    public void cancel() {
      running = false;
    }
  }
}
