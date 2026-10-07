package org.agentic.flink.memory.conversation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.UUID;
import org.agentic.flink.llm.ChatMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link ConversationStores#discover()} must skip a registered provider whose class fails to link
 * and keep going: a later working provider wins, and with no working provider the shared in-JVM
 * store is returned instead of the {@link NoClassDefFoundError} escaping.
 */
class ConversationStoresDiscoveryTest {

  static class MissingDriver {}

  static final class BrokenConversationStore extends MissingDriver {}

  /** A working provider registered after the broken one. */
  public static final class WorkingConversationStore implements ConversationStore {
    private static final long serialVersionUID = 1L;
    static final String MARKER = UUID.randomUUID().toString();
    private final InMemoryConversationStore delegate = new InMemoryConversationStore(16);

    public String marker() {
      return MARKER;
    }

    @Override
    public void append(String conversationId, ChatMessage message) {
      delegate.append(conversationId, message);
    }

    @Override
    public List<ChatMessage> history(String conversationId) {
      return delegate.history(conversationId);
    }

    @Override
    public int messageCount(String conversationId) {
      return delegate.messageCount(conversationId);
    }

    @Override
    public void putAttribute(String conversationId, String key, String value) {
      delegate.putAttribute(conversationId, key, value);
    }

    @Override
    public Optional<String> getAttribute(String conversationId, String key) {
      return delegate.getAttribute(conversationId, key);
    }

    @Override
    public Map<String, String> attributes(String conversationId) {
      return delegate.attributes(conversationId);
    }

    @Override
    public void associateUser(String conversationId, String userId) {
      delegate.associateUser(conversationId, userId);
    }

    @Override
    public Optional<String> userOf(String conversationId) {
      return delegate.userOf(conversationId);
    }

    @Override
    public List<String> conversationsForUser(String userId) {
      return delegate.conversationsForUser(userId);
    }

    @Override
    public void clear(String conversationId) {
      delegate.clear(conversationId);
    }

    @Override
    public List<String> conversations() {
      return delegate.conversations();
    }
  }

  @TempDir Path services;
  private ClassLoader previous;
  private Path serviceFile;

  @BeforeEach
  void install() throws IOException {
    Path dir = services.resolve("META-INF/services");
    Files.createDirectories(dir);
    serviceFile = dir.resolve(ConversationStore.class.getName());
    previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread()
        .setContextClassLoader(new BrokenProviderLoader(services.toUri().toURL(), previous));
  }

  @AfterEach
  void restore() {
    Thread.currentThread().setContextClassLoader(previous);
  }

  @Test
  void rawIterationFailsOnBrokenProvider() throws IOException {
    Files.writeString(
        serviceFile, BrokenConversationStore.class.getName() + "\n", StandardCharsets.UTF_8);
    assertThrows(
        NoClassDefFoundError.class,
        () -> ServiceLoader.load(ConversationStore.class).iterator().hasNext());
  }

  @Test
  void brokenProviderAloneFallsBackToShared() throws IOException {
    Files.writeString(
        serviceFile,
        BrokenConversationStore.class.getName()
            + "\norg.agentic.flink.memory.conversation.Missing"
            + UUID.randomUUID().toString().replace("-", "")
            + "\n",
        StandardCharsets.UTF_8);
    assertSame(InMemoryConversationStore.shared(), ConversationStores.discover());
  }

  @Test
  void brokenProviderDoesNotHideLaterWorkingProvider() throws IOException {
    Files.writeString(
        serviceFile,
        BrokenConversationStore.class.getName()
            + "\n"
            + WorkingConversationStore.class.getName()
            + "\n",
        StandardCharsets.UTF_8);
    ConversationStore store = ConversationStores.discover();
    WorkingConversationStore working = assertInstanceOf(WorkingConversationStore.class, store);
    assertEquals(WorkingConversationStore.MARKER, working.marker());
  }

  private static final class BrokenProviderLoader extends URLClassLoader {
    private final ClassLoader parent;

    BrokenProviderLoader(URL servicesDir, ClassLoader parent) {
      super(new URL[] {servicesDir}, parent);
      this.parent = parent;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.equals(MissingDriver.class.getName())) {
        throw new ClassNotFoundException(name);
      }
      if (name.equals(BrokenConversationStore.class.getName())) {
        synchronized (getClassLoadingLock(name)) {
          Class<?> loaded = findLoadedClass(name);
          if (loaded != null) {
            return loaded;
          }
          try (InputStream in = parent.getResourceAsStream(name.replace('.', '/') + ".class")) {
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
      if (name.equals("META-INF/services/" + ConversationStore.class.getName())) {
        return findResources(name);
      }
      return super.getResources(name);
    }
  }
}
