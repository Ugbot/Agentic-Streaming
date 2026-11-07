package org.agentic.flink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.agentic.flink.channel.FlussChannel;
import org.agentic.flink.channel.FlussSink;
import org.agentic.flink.memory.conversation.ConversationStores;
import org.agentic.flink.memory.conversation.InMemoryConversationStore;
import org.agentic.flink.memory.conversation.fluss.FlussConversationStore;
import org.agentic.flink.storage.StorageFactory;
import org.agentic.flink.storage.StorageTier;
import org.junit.jupiter.api.Test;

/**
 * The default uber jar has no Fluss client, but the test classpath does, because it is {@code
 * provided}. So each probe here runs in a class loader that hides {@code com.alibaba.fluss} and
 * redefines the framework classes, which is what a job on the default jar sees.
 */
class FlussClientAbsentTest {

  private static final String FLUSS = "com.alibaba.fluss.";

  @Test
  void presentOnTheTestClasspath() {
    assertTrue(FlussClient.isPresent());
  }

  @Test
  void absentWhenHidden() throws Exception {
    assertEquals("false", inHiddenLoader(Absent.class));
  }

  @Test
  void otherVectorStoresStayDiscoverable() throws Exception {
    List<String> stores = Arrays.asList(inHiddenLoader(ListVectorStores.class).split(","));
    assertTrue(stores.contains("InMemoryVectorStore"), stores.toString());
    assertTrue(stores.contains("PgVectorStore"), stores.toString());
  }

  @Test
  void flussVectorStoreSaysWhatToDo() throws Exception {
    assertEquals(FlussClient.MISSING_MESSAGE, inHiddenLoader(CreateVectorStore.class));
  }

  @Test
  void flussChannelSaysWhatToDo() throws Exception {
    assertEquals(FlussClient.MISSING_MESSAGE, inHiddenLoader(CreateChannel.class));
  }

  @Test
  void flussSinkSaysWhatToDo() throws Exception {
    assertEquals(FlussClient.MISSING_MESSAGE, inHiddenLoader(CreateSink.class));
  }

  @Test
  void flussConversationStoreSaysWhatToDo() throws Exception {
    assertEquals(FlussClient.MISSING_MESSAGE, inHiddenLoader(CreateConversationStore.class));
  }

  @Test
  void selectingFlussConversationsDoesNotFallBackToMemory() throws Exception {
    String key = "agentic.flink." + ConfigKeys.CONVERSATION_STORE;
    System.setProperty(key, "fluss");
    try {
      assertEquals(FlussClient.MISSING_MESSAGE, inHiddenLoader(DiscoverConversationStore.class));
    } finally {
      System.clearProperty(key);
    }
  }

  @Test
  void notSelectingFlussStillFallsBackToMemory() throws Exception {
    assertEquals(
        InMemoryConversationStore.class.getName(), inHiddenLoader(DiscoverConversationStore.class));
  }

  @Test
  void hiddenLoaderReallyHidesTheClient() {
    assertThrows(
        ClassNotFoundException.class,
        () -> new HidingLoader(FLUSS).loadClass("com.alibaba.fluss.client.Connection"));
    assertFalse(new HidingLoader(FLUSS).hides("org.agentic.flink.config.FlussClient"));
  }

  private static String inHiddenLoader(Class<? extends Callable<String>> probe) throws Exception {
    return HidingLoader.run(FLUSS, probe);
  }

  private static String messageOf(Callable<?> call) throws Exception {
    try {
      return "returned " + call.call();
    } catch (IllegalStateException e) {
      return e.getMessage();
    }
  }

  public static final class Absent implements Callable<String> {
    @Override
    public String call() {
      return String.valueOf(FlussClient.isPresent());
    }
  }

  public static final class ListVectorStores implements Callable<String> {
    @Override
    public String call() {
      return String.join(",", StorageFactory.getAvailableBackends(StorageTier.VECTOR));
    }
  }

  public static final class CreateVectorStore implements Callable<String> {
    @Override
    public String call() throws Exception {
      return messageOf(() -> StorageFactory.createVectorStore("fluss", Map.of()));
    }
  }

  public static final class CreateChannel implements Callable<String> {
    @Override
    public String call() throws Exception {
      return messageOf(() -> FlussChannel.of("localhost:9123", "db", "t", String.class));
    }
  }

  public static final class CreateSink implements Callable<String> {
    @Override
    public String call() throws Exception {
      return messageOf(() -> FlussSink.randomKey("localhost:9123", "db", "t"));
    }
  }

  public static final class CreateConversationStore implements Callable<String> {
    @Override
    public String call() throws Exception {
      return messageOf(() -> new FlussConversationStore("localhost:9123", "db", "t"));
    }
  }

  public static final class DiscoverConversationStore implements Callable<String> {
    @Override
    public String call() throws Exception {
      try {
        return ConversationStores.discover().getClass().getName();
      } catch (IllegalStateException e) {
        return e.getMessage();
      }
    }
  }
}
