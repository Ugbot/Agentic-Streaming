package org.agentic.flink.plugins.flintagents.examples;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.plugins.flintagents.action.ContextManagementActionWithStorage;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.ShortTermMemoryStore;
import org.agentic.flink.storage.StorageFactory;
import org.agentic.flink.storage.config.StorageConfiguration;
import java.util.*;
import org.apache.flink.agents.api.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example demonstrating the pluggable storage architecture.
 *
 * <p>Shows three usage patterns:
 *
 * <ol>
 *   <li>Direct storage API usage (standalone)
 *   <li>Programmatic storage configuration
 *   <li>Integration with Flink job and ContextManagementActionWithStorage
 * </ol>
 *
 * @author Agentic Flink Team
 */
public class PluggableStorageExample {

  public static void main(String[] args) throws Exception {
    // Example 1: Direct storage API usage
    directStorageExample();

    // Example 2: Programmatic configuration
    programmaticConfigExample();

    // Example 3: Flink job with pluggable storage (commented - requires data source)
    // flinkJobExample();
  }

  /**
   * Example 1: Direct storage API usage.
   *
   * <p>Shows how to use storage providers directly without Flink.
   */
  public static void directStorageExample() throws Exception {
    System.out.println("=== Example 1: Direct Storage API Usage ===\n");

    // Create in-memory storage (works immediately, no dependencies)
    Map<String, String> memoryConfig = new HashMap<>();
    memoryConfig.put("cache.max.size", "1000");
    memoryConfig.put("cache.ttl.seconds", "3600");

    ShortTermMemoryStore hotStore = StorageFactory.createShortTermStore("memory", memoryConfig);

    // Store context items
    List<ContextItem> items = new ArrayList<>();
    items.add(new ContextItem("User asked about order #12345", ContextPriority.MUST,
        MemoryType.SHORT_TERM));
    items.add(new ContextItem("User is premium tier", ContextPriority.SHOULD,
        MemoryType.SHORT_TERM));
    items.add(new ContextItem("Previous interaction was positive", ContextPriority.COULD,
        MemoryType.SHORT_TERM));

    String flowId = "flow-001";
    hotStore.putItems(flowId, items);
    System.out.println("Stored " + items.size() + " items for flow " + flowId);

    // Retrieve items
    List<ContextItem> retrieved = hotStore.getItems(flowId);
    System.out.println("Retrieved " + retrieved.size() + " items");

    // Get statistics
    Map<String, Object> stats = hotStore.getStatistics();
    System.out.println("Storage statistics: " + stats);

    // Cleanup
    hotStore.close();
    System.out.println("\n");
  }

  /**
   * Example 2: Programmatic configuration.
   *
   * <p>Shows how to configure multiple storage tiers programmatically.
   */
  public static void programmaticConfigExample() throws Exception {
    System.out.println("=== Example 2: Programmatic Configuration ===\n");

    // Configure HOT tier (in-memory)
    Map<String, String> hotConfig = new HashMap<>();
    hotConfig.put("cache.max.size", "10000");
    hotConfig.put("cache.ttl.seconds", "3600");

    // Configure WARM tier (would use Redis in production)
    // For this example, we'll just show the configuration structure
    Map<String, String> warmConfig = new HashMap<>();
    warmConfig.put("redis.host", "localhost");
    warmConfig.put("redis.port", "6379");
    warmConfig.put("redis.ttl.seconds", "86400");

    // Build storage configuration
    StorageConfiguration storageConfig =
        StorageConfiguration.builder()
            .withHotTier("memory", hotConfig)
            // .withWarmTier("redis", warmConfig)  // Uncomment when Redis available
            .build();

    System.out.println("Storage configuration created");
    System.out.println("HOT tier configured: " + storageConfig.isTierConfigured(
        org.agentic.flink.storage.StorageTier.HOT));
    System.out.println("WARM tier configured: " + storageConfig.isTierConfigured(
        org.agentic.flink.storage.StorageTier.WARM));

    // Create storage providers from configuration
    ShortTermMemoryStore hotStore = storageConfig.createShortTermStore();
    System.out.println("Created HOT tier store: " + hotStore.getProviderName());
    System.out.println("Expected latency: " + hotStore.getExpectedLatencyMs() + "ms");

    // Use the store
    String flowId = "flow-002";
    List<ContextItem> items = Arrays.asList(
        new ContextItem("Context from programmatic config", ContextPriority.MUST,
            MemoryType.SHORT_TERM)
    );

    hotStore.putItems(flowId, items);
    System.out.println("Stored items using configured storage");

    // Cleanup
    hotStore.close();
    System.out.println("\n");
  }

  /**
   * Example 3: Flink job with pluggable storage.
   *
   * <p>Shows how to integrate storage with a Flink streaming job.
   *
   * <p>Note: This is a template. In production, you would:
   *
   * <ul>
   *   <li>Replace mock source with Kafka or other real source
   *   <li>Load storage configuration from YAML file
   *   <li>Configure proper checkpointing
   *   <li>Add monitoring and metrics
   * </ul>
   */
  public static void flinkJobExample() throws Exception {
    System.out.println("=== Example 3: Flink Job with Pluggable Storage ===\n");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Configure storage (in-memory for this example)
    Map<String, String> hotConfig = new HashMap<>();
    hotConfig.put("cache.max.size", "10000");
    hotConfig.put("cache.ttl.seconds", "3600");

    StorageConfiguration storageConfig =
        StorageConfiguration.builder()
            .withHotTier("memory", hotConfig)
            .build();

    // Create data source (would be Kafka in production)
    // DataStream<Event> events = env.addSource(new KafkaSource(...));

    // For this example, we'll just show the pattern:
    // DataStream<Event> processed = events
    //     .keyBy(event -> (String) event.getAttr("flowId"))
    //     .process(new ContextManagementActionWithStorage("agent-001", storageConfig));

    System.out.println("Flink job structure defined");
    System.out.println("In production, this would process events with multi-tier storage");

    // env.execute("Pluggable Storage Example Job");
    System.out.println("\n");
  }

  /**
   * Example 4: Using WARM tier for conversation persistence.
   *
   * <p>Shows conversation resumption pattern (requires Redis or other WARM tier backend).
   */
  public static void warmTierExample() throws Exception {
    System.out.println("=== Example 4: WARM Tier Conversation Persistence ===\n");

    // This example requires Redis - uncomment when available

    /*
    Map<String, String> warmConfig = new HashMap<>();
    warmConfig.put("redis.host", "localhost");
    warmConfig.put("redis.port", "6379");
    warmConfig.put("redis.ttl.seconds", "86400");

    LongTermMemoryStore warmStore = StorageFactory.createLongTermStore("redis", warmConfig);

    // Save conversation context
    String flowId = "flow-003";
    String userId = "user-123";
    AgentContext context = new AgentContext("agent-001", flowId, userId, 4000, 50);

    warmStore.saveContext(flowId, context);
    System.out.println("Saved conversation context to WARM tier");

    // Save long-term facts
    Map<String, ContextItem> facts = new HashMap<>();
    facts.put("user_tier", new ContextItem("premium", ContextPriority.MUST, MemoryType.LONG_TERM));
    facts.put("language", new ContextItem("en", ContextPriority.SHOULD, MemoryType.LONG_TERM));

    warmStore.saveFacts(flowId, facts);
    System.out.println("Saved " + facts.size() + " long-term facts");

    // Later: Resume conversation
    Optional<AgentContext> resumed = warmStore.loadContext(flowId);
    if (resumed.isPresent()) {
        System.out.println("Successfully resumed conversation for flow " + flowId);

        Map<String, ContextItem> loadedFacts = warmStore.loadFacts(flowId);
        System.out.println("Loaded " + loadedFacts.size() + " facts");
    }

    warmStore.close();
    */

    System.out.println("WARM tier example requires Redis (code commented out)");
    System.out.println(
        "Add Jedis dependency and uncomment code in warmTierExample() to run\n");
  }
}
