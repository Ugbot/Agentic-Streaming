package org.jagentic.ports.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;

import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.jagentic.ports.quarkus.AgentMessages.AgentRequest;
import org.junit.jupiter.api.Test;

/** The Kafka value deserializer configured for the {@code requests} channel must be instantiable by
 * the Kafka client (public no-argument constructor) and round-trip the wire record. */
class AgentRequestDeserializerTest {

  @Test
  void kafkaClientCanInstantiateTheConfiguredDeserializer() throws ClassNotFoundException {
    Deserializer<?> instance = Utils.newInstance(AgentRequestDeserializer.class.getName(), Deserializer.class);
    assertNotNull(instance);
  }

  @Test
  void roundTripsAnAgentRequest() {
    AgentRequest request = new AgentRequest(
        "c-" + UUID.randomUUID(), "u-" + UUID.randomUUID(), "text " + UUID.randomUUID());
    try (ObjectMapperSerializer<AgentRequest> serializer = new ObjectMapperSerializer<>();
         AgentRequestDeserializer deserializer = new AgentRequestDeserializer()) {
      byte[] bytes = serializer.serialize("agent.requests", request);
      assertEquals(request, deserializer.deserialize("agent.requests", bytes));
    }
  }
}
