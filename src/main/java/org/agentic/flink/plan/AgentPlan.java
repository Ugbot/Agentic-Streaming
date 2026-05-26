package org.agentic.flink.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Declarative description of an agent operator. Python builds this from decorated user classes;
 * Java's {@code CompileUtils} consumes the JSON form via the PyFlink gateway and uses
 * {@link PlanReader} to instantiate the underlying SPIs and assemble an
 * {@link AgentPlanProcessFunction}.
 *
 * <p>Mirrors the upstream Apache Flink Agents plan shape but is keyed to this framework's SPIs.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class AgentPlan implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private final String agentId;
  private final String systemPrompt;
  private final ResourceSpec chatConnection;
  private final Map<String, String> chatSetup;
  private final List<ToolSpec> tools;
  private final List<ActionSpec> actions;
  private final Map<String, ResourceSpec> resources;
  private final List<ListenerSpec> listeners;

  @JsonCreator
  public AgentPlan(
      @JsonProperty("agent_id") String agentId,
      @JsonProperty("system_prompt") String systemPrompt,
      @JsonProperty("chat_connection") ResourceSpec chatConnection,
      @JsonProperty("chat_setup") Map<String, String> chatSetup,
      @JsonProperty("tools") List<ToolSpec> tools,
      @JsonProperty("actions") List<ActionSpec> actions,
      @JsonProperty("resources") Map<String, ResourceSpec> resources,
      @JsonProperty("listeners") List<ListenerSpec> listeners) {
    if (agentId == null || agentId.isEmpty()) {
      throw new IllegalArgumentException("AgentPlan.agent_id must be non-empty");
    }
    this.agentId = agentId;
    this.systemPrompt = systemPrompt;
    this.chatConnection = chatConnection;
    this.chatSetup = chatSetup == null ? Collections.emptyMap() : Map.copyOf(chatSetup);
    this.tools = tools == null ? Collections.emptyList() : List.copyOf(tools);
    this.actions = actions == null ? Collections.emptyList() : List.copyOf(actions);
    this.resources = resources == null ? Collections.emptyMap() : Map.copyOf(resources);
    this.listeners = listeners == null ? Collections.emptyList() : List.copyOf(listeners);
  }

  @JsonProperty("agent_id")
  public String getAgentId() {
    return agentId;
  }

  @JsonProperty("system_prompt")
  public String getSystemPrompt() {
    return systemPrompt;
  }

  @JsonProperty("chat_connection")
  public ResourceSpec getChatConnection() {
    return chatConnection;
  }

  @JsonProperty("chat_setup")
  public Map<String, String> getChatSetup() {
    return chatSetup;
  }

  @JsonProperty("tools")
  public List<ToolSpec> getTools() {
    return tools;
  }

  @JsonProperty("actions")
  public List<ActionSpec> getActions() {
    return actions;
  }

  @JsonProperty("resources")
  public Map<String, ResourceSpec> getResources() {
    return resources;
  }

  @JsonProperty("listeners")
  public List<ListenerSpec> getListeners() {
    return listeners;
  }

  public String toJson() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize AgentPlan to JSON", e);
    }
  }

  public static AgentPlan fromJson(String json) {
    try {
      return MAPPER.readValue(json, AgentPlan.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse AgentPlan JSON: " + e.getMessage(), e);
    }
  }

  public static ObjectMapper mapper() {
    return MAPPER;
  }
}
