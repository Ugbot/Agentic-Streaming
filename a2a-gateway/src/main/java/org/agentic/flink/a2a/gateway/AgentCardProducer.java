package org.agentic.flink.a2a.gateway;

import io.a2a.spec.AgentCapabilities;
import io.a2a.spec.AgentCard;
import io.a2a.spec.AgentInterface;
import io.a2a.server.PublicAgentCard;
import io.a2a.spec.AgentSkill;
import io.a2a.spec.TransportProtocol;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces the {@link AgentCard} served by the gateway at {@code /.well-known/agent-card.json},
 * built from {@link GatewayConfig}. Overrides the SDK's default-card producer.
 *
 * <p>Advertises streaming + push capabilities and the JSON-RPC interface at the configured public
 * URL. Skills come from {@code a2a.gateway.agent.skills} ({@code id:name:description} entries,
 * comma-separated); absent, a single generic skill is published.
 */
@ApplicationScoped
public class AgentCardProducer {
  private static final Logger LOG = LoggerFactory.getLogger(AgentCardProducer.class);

  @Produces
  @Singleton
  @PublicAgentCard
  public AgentCard agentCard(GatewayConfig config) {
    List<AgentInterface> interfaces = new ArrayList<>();
    interfaces.add(new AgentInterface(TransportProtocol.JSONRPC.asString(), config.publicUrl()));
    if (!config.grpcUrl().isBlank()) {
      interfaces.add(new AgentInterface(TransportProtocol.GRPC.asString(), config.grpcUrl()));
    }
    if (!config.restUrl().isBlank()) {
      interfaces.add(new AgentInterface(TransportProtocol.HTTP_JSON.asString(), config.restUrl()));
    }
    AgentCard card =
        AgentCard.builder()
            .name(config.agentName())
            .description(config.agentDescription())
            .version(config.agentVersion())
            .capabilities(new AgentCapabilities(true, true, false, List.of()))
            .defaultInputModes(List.of("text/plain", "application/json"))
            .defaultOutputModes(List.of("text/plain", "application/json"))
            .skills(parseSkills(config.skillsSpec(), config.agentId()))
            .supportedInterfaces(interfaces)
            .build();
    LOG.info("Serving A2A agent card '{}' at {}", config.agentName(), config.publicUrl());
    return card;
  }

  private static List<AgentSkill> parseSkills(String spec, String agentId) {
    List<AgentSkill> skills = new ArrayList<>();
    if (spec == null || spec.isBlank()) {
      skills.add(
          AgentSkill.builder()
              .id(agentId + "-default")
              .name("General")
              .description("General-purpose agent capability.")
              .tags(List.of("agent"))
              .build());
      return skills;
    }
    for (String entry : spec.split(",")) {
      String[] parts = entry.split(":", 3);
      if (parts.length >= 1 && !parts[0].isBlank()) {
        skills.add(
            AgentSkill.builder()
                .id(parts[0].trim())
                .name(parts.length > 1 ? parts[1].trim() : parts[0].trim())
                .description(parts.length > 2 ? parts[2].trim() : "")
                .tags(List.of("agent"))
                .build());
      }
    }
    return skills;
  }
}
