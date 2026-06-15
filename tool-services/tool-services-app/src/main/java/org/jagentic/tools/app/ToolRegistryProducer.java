package org.jagentic.tools.app;

import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import org.jagentic.core.ToolRegistry;
import org.jagentic.core.mcp.server.ToolServer;
import org.jagentic.tools.ToolPacks;

/** Builds the {@link ToolRegistry} from the configured pack selection and exposes it plus a
 * {@link ToolServer} as CDI singletons that every transport (MCP/REST/...) shares. */
@ApplicationScoped
public class ToolRegistryProducer {

  /** Comma-separated pack names to serve; empty/unset = all available packs. */
  @ConfigProperty(name = "tools.packs")
  Optional<String> packs;

  @ConfigProperty(name = "tools.server.name", defaultValue = "agentic-tool-services")
  String serverName;

  @ConfigProperty(name = "tools.server.version", defaultValue = "0.1.0")
  String serverVersion;

  @Produces
  @Singleton
  public ToolRegistry toolRegistry() {
    return ToolPacks.buildRegistryFromCsv(packs.orElse(""));
  }

  @Produces
  @Singleton
  public ToolServer toolServer(ToolRegistry registry) {
    return new ToolServer(registry, serverName, serverVersion);
  }
}
