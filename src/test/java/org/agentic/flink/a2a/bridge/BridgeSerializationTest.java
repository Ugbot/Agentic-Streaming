package org.agentic.flink.a2a.bridge;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The {@link A2ABridge} (and the {@code requestChannel()} source / {@code responseSink()} it
 * produces) ships in the Flink job graph, so the bridge and its Flink-facing factories must be
 * Java-serializable — a non-transient socket/pool/client would kill the job at submit. Guards the
 * inproc and redis transports (the gateway-side connector is intentionally NOT serializable and is
 * never shipped, so it isn't tested here).
 */
class BridgeSerializationTest {

  @SuppressWarnings("unchecked")
  private static <T> T roundTrip(T o) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(o);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return (T) ois.readObject();
    }
  }

  private static void assertBridgeShips(A2ABridge bridge) throws Exception {
    A2ABridge restored = roundTrip(bridge);
    assertInstanceOf(bridge.getClass(), restored);
    // The Flink-facing factories must also round-trip (they're what actually enters the job graph).
    assertNotNull(roundTrip(restored.requestChannel()));
    assertNotNull(roundTrip(restored.responseSink()));
  }

  @Test
  @DisplayName("in-proc bridge + its request channel / response sink are serializable")
  void inprocBridgeSerializable() throws Exception {
    assertBridgeShips(new InProcA2ABridge("req", "resp"));
  }

  @Test
  @DisplayName("redis bridge + its request channel / response sink are serializable")
  void redisBridgeSerializable() throws Exception {
    assertBridgeShips(new RedisA2ABridge("localhost", 6379, "a2a:req", "a2a:resp"));
  }
}
