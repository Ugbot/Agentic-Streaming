package org.agentic.flink.storage.toy;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.StorageFactory;
import org.agentic.flink.storage.StorageTier;
import org.agentic.flink.storage.config.StorageConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Executes the registration procedure from docs/guides/creating-storage-backends.md end to end:
 * implement {@link LongTermMemoryStore}, list the class in a META-INF/services file, and resolve it
 * through {@link StorageFactory} and {@link StorageConfiguration} without editing either class.
 */
class ToyBackendRegistrationTest {

  private static String randomToken() {
    return UUID.randomUUID().toString();
  }

  @Test
  @DisplayName("ServiceLoader discovers the toy provider from the test-scope service file")
  void serviceLoaderDiscoversToyProvider() {
    boolean found =
        ServiceLoader.load(LongTermMemoryStore.class).stream()
            .anyMatch(p -> p.type() == ToyLongTermStore.class);
    assertTrue(found, "ToyLongTermStore must be listed in META-INF/services");
  }

  @Test
  @DisplayName("StorageFactory resolves the provider name and propagates config")
  void factoryResolvesProviderName() throws Exception {
    String namespace = randomToken();
    Map<String, String> config = new HashMap<>();
    config.put(ToyLongTermStore.NAMESPACE_KEY, namespace);

    LongTermMemoryStore store =
        StorageFactory.createLongTermStore(ToyLongTermStore.PROVIDER_NAME, config);

    ToyLongTermStore toy = assertInstanceOf(ToyLongTermStore.class, store);
    assertEquals(namespace, toy.getNamespace());
    assertEquals(StorageTier.WARM, toy.getTier());
    toy.close();
  }

  @Test
  @DisplayName("StorageFactory also resolves the simple and fully qualified class names")
  void factoryResolvesClassNames() throws Exception {
    for (String name :
        Arrays.asList(
            ToyLongTermStore.class.getSimpleName(),
            ToyLongTermStore.class.getName(),
            ToyLongTermStore.PROVIDER_NAME.toUpperCase())) {
      LongTermMemoryStore store = StorageFactory.createLongTermStore(name, new HashMap<>());
      assertInstanceOf(ToyLongTermStore.class, store, name);
      store.close();
    }
  }

  @Test
  @DisplayName("getAvailableBackends(WARM) lists the class name but not the provider name")
  void availableBackendsListClassName() {
    List<String> warm = Arrays.asList(StorageFactory.getAvailableBackends(StorageTier.WARM));
    assertTrue(warm.contains(ToyLongTermStore.class.getSimpleName()), warm.toString());
    assertFalse(warm.contains(ToyLongTermStore.PROVIDER_NAME), warm.toString());
    assertTrue(
        StorageFactory.isBackendAvailable(StorageTier.WARM, "toylongtermstore"),
        "availability check is case-insensitive");
    assertArrayEquals(
        new String[] {"memory"}, StorageFactory.getAvailableBackends(StorageTier.HOT));
  }

  @Test
  @DisplayName("StorageConfiguration validates the class name and creates the toy store")
  void storageConfigurationUsesClassName() throws Exception {
    String namespace = randomToken();
    Map<String, String> config = Map.of(ToyLongTermStore.NAMESPACE_KEY, namespace);

    assertThrows(
        IllegalStateException.class,
        () ->
            StorageConfiguration.builder()
                .withWarmTier(ToyLongTermStore.PROVIDER_NAME, config)
                .build(),
        "validate() consults getAvailableBackends, which does not carry the provider name");

    StorageConfiguration storage =
        StorageConfiguration.builder()
            .withWarmTier(ToyLongTermStore.class.getSimpleName(), config)
            .build();
    LongTermMemoryStore store = storage.createLongTermStore();
    ToyLongTermStore toy = assertInstanceOf(ToyLongTermStore.class, store);
    assertEquals(namespace, toy.getNamespace());
    toy.close();
  }

  @Test
  @DisplayName("Toy store round-trips context and facts and survives Java serialization")
  void roundTripAndSerialization() throws Exception {
    LongTermMemoryStore store =
        StorageFactory.createLongTermStore(ToyLongTermStore.PROVIDER_NAME, new HashMap<>());
    String flowId = randomToken();
    String userId = randomToken();
    int maxTokens = ThreadLocalRandom.current().nextInt(100, 10_000);
    AgentContext context = new AgentContext(randomToken(), flowId, userId, maxTokens, 50);
    ContextItem fact = new ContextItem(randomToken(), ContextPriority.MUST, MemoryType.LONG_TERM);

    store.saveContext(flowId, context);
    store.addFact(flowId, "fact", fact);

    Optional<AgentContext> loaded = store.loadContext(flowId);
    assertTrue(loaded.isPresent());
    assertEquals(flowId, loaded.get().getFlowId());
    assertEquals(userId, loaded.get().getUserId());
    assertEquals(fact.getContent(), store.loadFacts(flowId).get("fact").getContent());
    assertEquals(List.of(flowId), store.listConversationsForUser(userId));

    store.deleteConversation(flowId);
    assertFalse(store.conversationExists(flowId));
    assertTrue(store.loadFacts(flowId).isEmpty());

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(store);
    }
    Object copy;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = in.readObject();
    }
    LongTermMemoryStore reopened = assertInstanceOf(ToyLongTermStore.class, copy);
    reopened.initialize(new HashMap<>());
    assertFalse(reopened.conversationExists(flowId));
    reopened.close();
    store.close();
  }
}
