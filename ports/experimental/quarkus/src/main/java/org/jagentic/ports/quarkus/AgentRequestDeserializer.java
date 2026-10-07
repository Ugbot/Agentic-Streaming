package org.jagentic.ports.quarkus;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

import org.jagentic.ports.quarkus.AgentMessages.AgentRequest;

/**
 * Kafka value deserializer for the {@code requests} channel. {@link ObjectMapperDeserializer} is
 * abstract, so the connector needs this concrete, no-argument-constructible subclass.
 */
public class AgentRequestDeserializer extends ObjectMapperDeserializer<AgentRequest> {
  public AgentRequestDeserializer() {
    super(AgentRequest.class);
  }
}
