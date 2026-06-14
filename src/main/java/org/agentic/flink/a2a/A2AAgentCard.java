package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The A2A discovery document served at {@code /.well-known/agent-card.json}.
 *
 * <p>A lean, framework-internal projection of the protocol {@code AgentCard}: enough to discover a
 * peer's endpoint, transports, capabilities, and skills (outbound), and to publish our own agents
 * (the Quarkus gateway). The SDK adapter and gateway translate between this and the SDK
 * {@code AgentCard}; everything else uses this type. Immutable + {@link Serializable}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class A2AAgentCard implements Serializable {
  private static final long serialVersionUID = 1L;

  /** {@code {url, transport}} pair from {@code additionalInterfaces}. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static final class Interface implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String url;
    private final A2ATransport transport;

    public Interface(String url, A2ATransport transport) {
      this.url = Objects.requireNonNull(url, "url");
      this.transport = transport == null ? A2ATransport.JSONRPC : transport;
    }

    public String getUrl() {
      return url;
    }

    public A2ATransport getTransport() {
      return transport;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Interface)) {
        return false;
      }
      Interface that = (Interface) o;
      return Objects.equals(url, that.url) && transport == that.transport;
    }

    @Override
    public int hashCode() {
      return Objects.hash(url, transport);
    }
  }

  /** Boolean capability flags from the {@code capabilities} object. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static final class Capabilities implements Serializable {
    private static final long serialVersionUID = 1L;
    private final boolean streaming;
    private final boolean pushNotifications;
    private final boolean stateTransitionHistory;

    public Capabilities(boolean streaming, boolean pushNotifications, boolean stateTransitionHistory) {
      this.streaming = streaming;
      this.pushNotifications = pushNotifications;
      this.stateTransitionHistory = stateTransitionHistory;
    }

    public boolean isStreaming() {
      return streaming;
    }

    public boolean isPushNotifications() {
      return pushNotifications;
    }

    public boolean isStateTransitionHistory() {
      return stateTransitionHistory;
    }
  }

  private final String protocolVersion;
  private final String name;
  private final String description;
  private final String url;
  private final A2ATransport preferredTransport;
  private final List<Interface> additionalInterfaces;
  private final String version;
  private final Capabilities capabilities;
  private final List<String> defaultInputModes;
  private final List<String> defaultOutputModes;
  private final List<A2AAgentSkill> skills;
  private final Map<String, Object> securitySchemes;

  public A2AAgentCard(
      String protocolVersion,
      String name,
      String description,
      String url,
      A2ATransport preferredTransport,
      List<Interface> additionalInterfaces,
      String version,
      Capabilities capabilities,
      List<String> defaultInputModes,
      List<String> defaultOutputModes,
      List<A2AAgentSkill> skills,
      Map<String, Object> securitySchemes) {
    this.protocolVersion = protocolVersion == null ? "1.0" : protocolVersion;
    this.name = Objects.requireNonNull(name, "name");
    this.description = description == null ? "" : description;
    this.url = url;
    this.preferredTransport = preferredTransport == null ? A2ATransport.JSONRPC : preferredTransport;
    this.additionalInterfaces = copy(additionalInterfaces);
    this.version = version == null ? "0.0.0" : version;
    this.capabilities = capabilities == null ? new Capabilities(false, false, false) : capabilities;
    this.defaultInputModes = copyStr(defaultInputModes);
    this.defaultOutputModes = copyStr(defaultOutputModes);
    this.skills = skills == null ? Collections.emptyList() : List.copyOf(skills);
    this.securitySchemes =
        securitySchemes == null
            ? null
            : Collections.unmodifiableMap(new LinkedHashMap<>(securitySchemes));
  }

  private static List<Interface> copy(List<Interface> in) {
    return in == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(in));
  }

  private static List<String> copyStr(List<String> in) {
    return in == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(in));
  }

  public String getProtocolVersion() {
    return protocolVersion;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getUrl() {
    return url;
  }

  public A2ATransport getPreferredTransport() {
    return preferredTransport;
  }

  public List<Interface> getAdditionalInterfaces() {
    return additionalInterfaces;
  }

  public String getVersion() {
    return version;
  }

  public Capabilities getCapabilities() {
    return capabilities;
  }

  public List<String> getDefaultInputModes() {
    return defaultInputModes;
  }

  public List<String> getDefaultOutputModes() {
    return defaultOutputModes;
  }

  public List<A2AAgentSkill> getSkills() {
    return skills;
  }

  public Map<String, Object> getSecuritySchemes() {
    return securitySchemes;
  }

  /** Find the advertised skill with the given id, if present. */
  public Optional<A2AAgentSkill> skill(String skillId) {
    return skills.stream().filter(s -> s.getId().equals(skillId)).findFirst();
  }

  /**
   * Resolve the endpoint URL to use for a given transport: the matching {@code additionalInterfaces}
   * entry, or the primary {@link #getUrl()} when it is the preferred transport.
   */
  public Optional<String> endpointFor(A2ATransport transport) {
    if (transport == preferredTransport && url != null) {
      return Optional.of(url);
    }
    for (Interface iface : additionalInterfaces) {
      if (iface.getTransport() == transport) {
        return Optional.of(iface.getUrl());
      }
    }
    return url != null && transport == preferredTransport ? Optional.of(url) : Optional.empty();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Fluent builder used by the gateway's {@code AgentCardProducer}. */
  public static final class Builder {
    private String protocolVersion = "1.0";
    private String name;
    private String description;
    private String url;
    private A2ATransport preferredTransport = A2ATransport.JSONRPC;
    private final List<Interface> additionalInterfaces = new ArrayList<>();
    private String version = "0.0.0";
    private Capabilities capabilities = new Capabilities(false, false, false);
    private List<String> defaultInputModes;
    private List<String> defaultOutputModes;
    private final List<A2AAgentSkill> skills = new ArrayList<>();
    private Map<String, Object> securitySchemes;

    public Builder protocolVersion(String v) {
      this.protocolVersion = v;
      return this;
    }

    public Builder name(String v) {
      this.name = v;
      return this;
    }

    public Builder description(String v) {
      this.description = v;
      return this;
    }

    public Builder url(String v) {
      this.url = v;
      return this;
    }

    public Builder preferredTransport(A2ATransport v) {
      this.preferredTransport = v;
      return this;
    }

    public Builder addInterface(String url, A2ATransport transport) {
      this.additionalInterfaces.add(new Interface(url, transport));
      return this;
    }

    public Builder version(String v) {
      this.version = v;
      return this;
    }

    public Builder capabilities(boolean streaming, boolean push, boolean stateHistory) {
      this.capabilities = new Capabilities(streaming, push, stateHistory);
      return this;
    }

    public Builder defaultInputModes(List<String> v) {
      this.defaultInputModes = v;
      return this;
    }

    public Builder defaultOutputModes(List<String> v) {
      this.defaultOutputModes = v;
      return this;
    }

    public Builder addSkill(A2AAgentSkill skill) {
      this.skills.add(skill);
      return this;
    }

    public Builder securitySchemes(Map<String, Object> v) {
      this.securitySchemes = v;
      return this;
    }

    public A2AAgentCard build() {
      return new A2AAgentCard(
          protocolVersion,
          name,
          description,
          url,
          preferredTransport,
          additionalInterfaces,
          version,
          capabilities,
          defaultInputModes,
          defaultOutputModes,
          skills,
          securitySchemes);
    }
  }
}
