package org.agentic.flink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.agentic.flink.storage.StorageFactory;
import org.agentic.flink.storage.StorageTier;
import org.agentic.flink.storage.vector.MilvusVectorStore;
import org.junit.jupiter.api.Test;

/**
 * The default uber jar has no Milvus SDK, but the test classpath does, because it is {@code
 * provided}. So each probe here runs in a class loader that hides {@code io.milvus}.
 */
class MilvusClientAbsentTest {

  private static final String MILVUS = "io.milvus.";

  @Test
  void presentOnTheTestClasspath() {
    assertTrue(MilvusClient.isPresent());
  }

  @Test
  void absentWhenHidden() throws Exception {
    assertEquals("false", HidingLoader.run(MILVUS, Absent.class));
  }

  @Test
  void otherVectorStoresStayDiscoverable() throws Exception {
    List<String> stores =
        Arrays.asList(HidingLoader.run(MILVUS, ListVectorStores.class).split(","));
    assertTrue(stores.contains("InMemoryVectorStore"), stores.toString());
    assertTrue(stores.contains("QdrantVectorStore"), stores.toString());
  }

  @Test
  void selectingMilvusByProviderNameSaysWhatToDo() throws Exception {
    assertEquals(MilvusClient.MISSING_MESSAGE, HidingLoader.run(MILVUS, ByProviderName.class));
  }

  @Test
  void selectingMilvusByClassNameSaysWhatToDo() throws Exception {
    assertEquals(MilvusClient.MISSING_MESSAGE, HidingLoader.run(MILVUS, ByClassName.class));
  }

  @Test
  void theSdkCheckComesBeforeTheConfigCheck() throws Exception {
    assertEquals(MilvusClient.MISSING_MESSAGE, HidingLoader.run(MILVUS, WithoutDimension.class));
  }

  @Test
  void hiddenLoaderReallyHidesTheSdk() {
    assertThrows(
        ClassNotFoundException.class,
        () -> new HidingLoader(MILVUS).loadClass("io.milvus.v2.client.MilvusClientV2"));
    assertFalse(new HidingLoader(MILVUS).hides("org.agentic.flink.config.MilvusClient"));
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
      return String.valueOf(MilvusClient.isPresent());
    }
  }

  public static final class ListVectorStores implements Callable<String> {
    @Override
    public String call() {
      return String.join(",", StorageFactory.getAvailableBackends(StorageTier.VECTOR));
    }
  }

  public static final class ByProviderName implements Callable<String> {
    @Override
    public String call() throws Exception {
      return messageOf(
          () -> StorageFactory.createVectorStore("milvus", Map.of("vector.dimension", "4")));
    }
  }

  public static final class ByClassName implements Callable<String> {
    @Override
    public String call() throws Exception {
      return messageOf(
          () ->
              StorageFactory.createVectorStore(
                  MilvusVectorStore.class.getName(), Map.of("vector.dimension", "4")));
    }
  }

  public static final class WithoutDimension implements Callable<String> {
    @Override
    public String call() throws Exception {
      return messageOf(() -> StorageFactory.createVectorStore("milvus", Map.of()));
    }
  }
}
