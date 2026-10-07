package org.agentic.flink.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.UUID;
import org.agentic.flink.storage.memory.InMemoryLongTermStore;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link StorageFactory} must keep iterating the {@link VectorStore} and {@link
 * LongTermMemoryStore} SPIs when one registered provider fails to link (an optional backend on the
 * classpath without its dependency). The broken provider here is a real linkage failure: its
 * superclass is hidden from the class loader, so defining the class throws {@link
 * NoClassDefFoundError}, which {@link ServiceLoader} reports as {@link ServiceConfigurationError}.
 */
class StorageFactoryServiceLoaderTest {

  /** Stands in for the missing optional dependency (never loadable through the test loader). */
  static class MissingDependency {}

  /** Provider whose class cannot be defined because its superclass is missing. */
  static final class BrokenVectorStore extends MissingDependency {}

  /** Same failure mode for the long-term store SPI. */
  static final class BrokenLongTermStore extends MissingDependency {}

  @TempDir Path services;

  private ClassLoader previous;
  private String randomProvider;

  @BeforeEach
  void installBrokenProviders() throws IOException {
    randomProvider =
        "org.agentic.flink.storage.Missing" + UUID.randomUUID().toString().replace("-", "");
    Path dir = services.resolve("META-INF/services");
    Files.createDirectories(dir);
    Files.writeString(
        dir.resolve(VectorStore.class.getName()),
        String.join(
            "\n",
            BrokenVectorStore.class.getName(),
            randomProvider,
            InMemoryVectorStore.class.getName(),
            ""),
        StandardCharsets.UTF_8);
    Files.writeString(
        dir.resolve(LongTermMemoryStore.class.getName()),
        String.join(
            "\n",
            BrokenLongTermStore.class.getName(),
            randomProvider,
            InMemoryLongTermStore.class.getName(),
            ""),
        StandardCharsets.UTF_8);
    previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread()
        .setContextClassLoader(new BrokenProviderLoader(services.toUri().toURL(), previous));
  }

  @AfterEach
  void restoreLoader() {
    Thread.currentThread().setContextClassLoader(previous);
  }

  @Test
  void rawServiceLoaderIterationFailsOnTheBrokenProvider() {
    Throwable e =
        assertThrows(
            Throwable.class,
            () -> {
              for (VectorStore ignored : ServiceLoader.load(VectorStore.class)) {
                // iterate to the broken entry
              }
            });
    assertTrue(
        e instanceof NoClassDefFoundError || e instanceof ServiceConfigurationError,
        String.valueOf(e));
    assertTrue(String.valueOf(e).contains("MissingDependency"), String.valueOf(e));
  }

  @Test
  void rawServiceLoaderIterationFailsOnTheMissingProviderClass() {
    Path dir = services.resolve("META-INF/services");
    assertThrows(
        ServiceConfigurationError.class,
        () -> {
          Files.writeString(
              dir.resolve(VectorStore.class.getName()),
              randomProvider + "\n" + InMemoryVectorStore.class.getName() + "\n",
              StandardCharsets.UTF_8);
          for (VectorStore ignored : ServiceLoader.load(VectorStore.class)) {
            // iterate to the missing entry
          }
        });
    List<String> names = new ArrayList<>();
    for (VectorStore s : StorageFactory.providers(VectorStore.class)) {
      names.add(s.getClass().getSimpleName());
    }
    assertEquals(List.of("InMemoryVectorStore"), names);
  }

  @Test
  void vectorStoreCreationSkipsBrokenProviderAndFindsTheNextOne() throws Exception {
    VectorStore store =
        StorageFactory.createVectorStore("in-memory", Map.of("vector.dimension", "8"));
    assertInstanceOf(InMemoryVectorStore.class, store);
  }

  @Test
  void vectorBackendsListOmitsBrokenProvider() {
    List<String> vec = List.of(StorageFactory.getAvailableBackends(StorageTier.VECTOR));
    assertTrue(vec.contains("InMemoryVectorStore"), String.valueOf(vec));
    assertFalse(vec.contains("BrokenVectorStore"), String.valueOf(vec));
    assertTrue(StorageFactory.isBackendAvailable(StorageTier.VECTOR, "InMemoryVectorStore"));
  }

  @Test
  void unknownVectorBackendStillFailsWithIllegalArgument() {
    String unknown = "nope-" + UUID.randomUUID();
    assertThrows(
        IllegalArgumentException.class, () -> StorageFactory.createVectorStore(unknown, Map.of()));
  }

  @Test
  void longTermStoreDiscoverySkipsBrokenProvider() throws Exception {
    LongTermMemoryStore store =
        StorageFactory.createLongTermStore("InMemoryLongTermStore", Map.of());
    assertInstanceOf(InMemoryLongTermStore.class, store);
    List<String> warm = List.of(StorageFactory.getAvailableBackends(StorageTier.WARM));
    assertTrue(warm.contains("InMemoryLongTermStore"), String.valueOf(warm));
    assertFalse(warm.contains("BrokenLongTermStore"), String.valueOf(warm));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> StorageFactory.createLongTermStore("unknown-" + UUID.randomUUID(), Map.of()));
    assertTrue(e.getMessage().contains("InMemoryLongTermStore"), e.getMessage());
  }

  @Test
  void guardedIteratorYieldsOnlyLoadableProviders() {
    List<String> names = new ArrayList<>();
    for (VectorStore s : StorageFactory.providers(VectorStore.class)) {
      names.add(s.getClass().getSimpleName());
    }
    assertEquals(List.of("InMemoryVectorStore"), names);
  }

  /**
   * Serves the temp-dir service files instead of the real ones, defines the two broken provider
   * classes itself (so their superclass is resolved through this loader) and refuses to load {@link
   * MissingDependency}, which turns the definition into a {@link NoClassDefFoundError}.
   */
  private static final class BrokenProviderLoader extends URLClassLoader {
    private final ClassLoader parent;

    BrokenProviderLoader(URL servicesDir, ClassLoader parent) {
      super(new URL[] {servicesDir}, parent);
      this.parent = parent;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.equals(MissingDependency.class.getName())) {
        throw new ClassNotFoundException(name);
      }
      if (name.equals(BrokenVectorStore.class.getName())
          || name.equals(BrokenLongTermStore.class.getName())) {
        synchronized (getClassLoadingLock(name)) {
          Class<?> loaded = findLoadedClass(name);
          if (loaded != null) {
            return loaded;
          }
          String resource = name.replace('.', '/') + ".class";
          try (InputStream in = parent.getResourceAsStream(resource)) {
            if (in == null) {
              throw new ClassNotFoundException(name);
            }
            byte[] bytes = in.readAllBytes();
            return defineClass(name, bytes, 0, bytes.length);
          } catch (IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
      }
      return super.loadClass(name, resolve);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      if (name.equals("META-INF/services/" + VectorStore.class.getName())
          || name.equals("META-INF/services/" + LongTermMemoryStore.class.getName())) {
        return findResources(name);
      }
      return super.getResources(name);
    }
  }
}
