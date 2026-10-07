package org.agentic.flink.function;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.ServiceConfigurationError;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.Test;

/** {@link ChatConnections} skips providers that fail to load and still finds a later one. */
class ChatConnectionsTest {

  private static final class Stub implements ChatConnection {
    private static final long serialVersionUID = 1L;

    @Override
    public ChatClient bind(RuntimeContext runtimeContext) {
      throw new UnsupportedOperationException();
    }
  }

  /** Each step either yields a provider or throws; mirrors ServiceLoader's lazy iterator. */
  private static Iterator<ChatConnection> providers(List<Supplier<ChatConnection>> steps) {
    return new Iterator<>() {
      int i = 0;

      @Override
      public boolean hasNext() {
        return i < steps.size();
      }

      @Override
      public ChatConnection next() {
        if (i >= steps.size()) {
          throw new NoSuchElementException();
        }
        return steps.get(i++).get();
      }
    };
  }

  private static Supplier<ChatConnection> broken() {
    return ThreadLocalRandom.current().nextBoolean()
        ? () -> {
          throw new ServiceConfigurationError("provider " + ThreadLocalRandom.current().nextInt());
        }
        : () -> {
          throw new NoClassDefFoundError("optional/Dep" + ThreadLocalRandom.current().nextInt());
        };
  }

  @Test
  void skipsBrokenProvidersAndReturnsTheFirstLoadableOne() {
    int broken = ThreadLocalRandom.current().nextInt(1, 6);
    List<Supplier<ChatConnection>> steps = new ArrayList<>();
    for (int i = 0; i < broken; i++) {
      steps.add(broken());
    }
    Stub good = new Stub();
    steps.add(() -> good);
    steps.add(() -> new Stub());

    assertSame(good, ChatConnections.firstLoadable(providers(steps)));
  }

  @Test
  void returnsNullWhenEveryProviderIsBroken() {
    int broken = ThreadLocalRandom.current().nextInt(1, 6);
    List<Supplier<ChatConnection>> steps = new ArrayList<>();
    for (int i = 0; i < broken; i++) {
      steps.add(broken());
    }
    assertNull(ChatConnections.firstLoadable(providers(steps)));
  }

  @Test
  void hasNextFailuresAreSkippedToo() {
    Stub good = new Stub();
    Iterator<ChatConnection> it =
        new Iterator<>() {
          int calls = 0;

          @Override
          public boolean hasNext() {
            if (calls++ == 0) {
              throw new ServiceConfigurationError("bad META-INF/services entry");
            }
            return calls == 2;
          }

          @Override
          public ChatConnection next() {
            return good;
          }
        };
    assertSame(good, ChatConnections.firstLoadable(it));
  }

  @Test
  void defaultProviderIsDiscoveredOnTheTestClasspath() {
    assertNotNull(ChatConnections.discover());
    assertNotNull(ChatConnections.require("test"));
  }
}
