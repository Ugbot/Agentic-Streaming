package org.jagentic.tools.app.pubsub;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.arc.properties.IfBuildProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import org.jagentic.core.ToolRegistry;

/**
 * Async Kafka pub-sub transport: consume a request from the {@code tool-requests} channel, run
 * the tool, and emit the result to the {@code tool-results} channel. Request/result are JSON
 * strings ({@code {id, tool, args}} -&gt; {@code {id, ok, result|error}}), correlated by {@code id}.
 *
 * <p><b>Off by default.</b> This bean only exists when the build property
 * {@code tools.kafka.enabled=true} is set ({@code enableIfMissing=false}), so the default build
 * and the existing endpoint tests boot with no Kafka connector wired and never attempt to reach a
 * broker. To enable, build/run with {@code -Dtools.kafka.enabled=true} and supply the channel
 * config (see {@code application-kafka.properties}):
 *
 * <pre>
 *   kafka.bootstrap.servers=localhost:9092
 *   mp.messaging.incoming.tool-requests.connector=smallrye-kafka
 *   mp.messaging.incoming.tool-requests.topic=tool-requests
 *   mp.messaging.incoming.tool-requests.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 *   mp.messaging.outgoing.tool-results.connector=smallrye-kafka
 *   mp.messaging.outgoing.tool-results.topic=tool-results
 *   mp.messaging.outgoing.tool-results.value.serializer=org.apache.kafka.common.serialization.StringSerializer
 * </pre>
 */
@ApplicationScoped
@IfBuildProperty(name = "tools.kafka.enabled", stringValue = "true", enableIfMissing = false)
public class KafkaToolBridge {

  @Inject
  ToolRegistry registry;

  @Inject
  ObjectMapper mapper;

  private ToolPubSub pubsub;

  @PostConstruct
  void init() {
    this.pubsub = new ToolPubSub(registry, mapper);
  }

  @Incoming("tool-requests")
  @Outgoing("tool-results")
  public String onRequest(String requestJson) {
    return pubsub.handle(requestJson);
  }
}
