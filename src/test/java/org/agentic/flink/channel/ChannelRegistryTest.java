package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

class ChannelRegistryTest {

  private static List<String> randomNames(int n) {
    List<String> names = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      names.add("channel-" + UUID.randomUUID());
    }
    return names;
  }

  @Test
  void lookupByNameReturnsTheRegisteredChannelAndPreservesInsertionOrder() {
    int n = ThreadLocalRandom.current().nextInt(2, 8);
    List<String> names = randomNames(n);
    List<StaticSeedChannel<String>> channels = new ArrayList<>();
    ChannelRegistry.Builder builder = ChannelRegistry.builder();
    for (String name : names) {
      StaticSeedChannel<String> channel = new StaticSeedChannel<>(List.of(name), Types.STRING);
      channels.add(channel);
      builder.add(name, channel);
    }
    ChannelRegistry registry = builder.build();

    assertEquals(n, registry.size());
    assertEquals(names, new ArrayList<>(registry.all().keySet()));
    for (int i = 0; i < n; i++) {
      assertSame(channels.get(i), registry.<String>require(names.get(i)));
      assertSame(channels.get(i), registry.<String>get(names.get(i)).orElseThrow());
    }
  }

  @Test
  void missingNameIsEmptyForGetAndFailsForRequire() {
    ChannelRegistry registry =
        ChannelRegistry.builder()
            .add("present", new StaticSeedChannel<>(List.of("x"), Types.STRING))
            .build();
    String missing = "missing-" + UUID.randomUUID();

    assertTrue(registry.get(missing).isEmpty());
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> registry.require(missing));
    assertTrue(e.getMessage().contains(missing));
  }

  @Test
  void allIsReadOnlyAndLaterBuilderChangesDoNotLeakIntoABuiltRegistry() {
    ChannelRegistry.Builder builder =
        ChannelRegistry.builder().add("a", new StaticSeedChannel<>(List.of("a"), Types.STRING));
    ChannelRegistry registry = builder.build();
    builder.add("b", new StaticSeedChannel<>(List.of("b"), Types.STRING));

    assertEquals(1, registry.size());
    assertFalse(registry.all().containsKey("b"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> registry.all().put("c", new StaticSeedChannel<>(List.of("c"), Types.STRING)));
  }

  @Test
  void registryShipsWithTheJobGraphViaJavaSerialization() throws Exception {
    String name = "seed-" + UUID.randomUUID();
    ChannelRegistry registry =
        ChannelRegistry.builder()
            .add(name, new StaticSeedChannel<>(List.of("one", "two"), Types.STRING))
            .build();

    ChannelRegistry copy =
        InstantiationUtil.deserializeObject(
            InstantiationUtil.serializeObject(registry), getClass().getClassLoader());

    assertEquals(1, copy.size());
    assertEquals("static-seed", copy.require(name).providerName());
  }
}
