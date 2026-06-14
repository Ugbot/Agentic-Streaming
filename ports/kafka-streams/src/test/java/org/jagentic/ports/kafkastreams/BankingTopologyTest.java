package org.jagentic.ports.kafkastreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Agent;
import org.jagentic.core.Banking;
import org.jagentic.core.Brain;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;

/** Runs the topology with no broker via TopologyTestDriver: the default banking graph
 * routes + the get_balance tool fires, and an INJECTED extended graph (new fraud path +
 * tool) flows through the same Kafka Streams seam. */
class BankingTopologyTest {

  private static Properties props() {
    Properties p = new Properties();
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "agentic-ks-test");
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return p;
  }

  private static String runOnce(Topology topology, String cid, String text) {
    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, String> in = driver.createInputTopic(
          BankingTopology.REQUESTS_TOPIC, new StringSerializer(), new StringSerializer());
      TestOutputTopic<String, String> out = driver.createOutputTopic(
          BankingTopology.RESPONSES_TOPIC, new StringDeserializer(), new StringDeserializer());
      in.pipeInput(cid, text);
      return out.readValue();
    }
  }

  @Test
  void defaultBankingGraphRoutesAndCallsTool() {
    String reply = runOnce(BankingTopology.build(), "c1", "what is my balance?");
    assertTrue(reply.startsWith("[payments]"), reply);
    assertTrue(reply.contains("1234.56"), reply);
  }

  @Test
  void extendedGraphFlowsThroughTheKafkaStreamsSeam() {
    ToolRegistry tools = Banking.defaultTools()
        .register("freeze_card", "Freeze the user's card", p -> "FRZ-" + p.get("user"));
    Brain fraud = (userText, ctx) -> {
      Object ref = ctx.callTool("freeze_card", Map.of("user", ctx.userId));
      return "[fraud] Your card is frozen (ref " + ref + ").";
    };
    Map<String, Agent> paths = new LinkedHashMap<>();
    paths.put("cards", new Agent("cards", "c", new Banking.RuleBrain("cards")));
    paths.put("payments", new Agent("payments", "p", new Banking.RuleBrain("payments")));
    paths.put("general", new Agent("general", "g", new Banking.RuleBrain("general")));
    paths.put("fraud", new Agent("fraud", "f", fraud));
    RoutedGraph extended = new RoutedGraph(
        (ev, ctx) -> {
          String low = ev.text().toLowerCase();
          return (low.contains("stolen") || low.contains("freeze")) ? "fraud" : Banking.router(ev, ctx);
        },
        paths,
        (reply, ctx) -> new RoutedGraph.Verifier.Result(reply.startsWith("["), reply));

    Topology topology = BankingTopology.build(extended, tools, Banking.retriever());
    String reply = runOnce(topology, "c1", "my card was stolen, please freeze it");
    assertEquals(true, reply.startsWith("[fraud]"));
    assertTrue(reply.contains("FRZ-user-c1"), reply);
  }
}
