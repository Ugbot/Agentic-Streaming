package org.agentic.flink.langchain;

import dev.langchain4j.model.input.Prompt;
import dev.langchain4j.model.input.PromptTemplate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralized manager for prompt templates using LangChain4j PromptTemplate.
 *
 * <p>This manager provides:
 * <ul>
 *   <li>Pre-defined prompt templates for common agent tasks
 *   <li>Template registration and retrieval
 *   <li>Variable substitution using LangChain4j PromptTemplate
 *   <li>Template caching for performance
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * PromptTemplateManager manager = PromptTemplateManager.getInstance();
 * Map&lt;String, Object&gt; vars = new HashMap&lt;&gt;();
 * vars.put("result", "42");
 * Prompt prompt = manager.renderTemplate("validation", vars);
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class PromptTemplateManager {

  private static final Logger LOG = LoggerFactory.getLogger(PromptTemplateManager.class);

  private static final PromptTemplateManager INSTANCE = new PromptTemplateManager();

  private final Map<String, PromptTemplate> templates = new ConcurrentHashMap<>();

  /**
   * Private constructor - use getInstance() instead.
   */
  private PromptTemplateManager() {
    registerDefaultTemplates();
  }

  /**
   * Gets the singleton instance of PromptTemplateManager.
   *
   * @return The singleton instance
   */
  public static PromptTemplateManager getInstance() {
    return INSTANCE;
  }

  /**
   * Registers the default prompt templates.
   */
  private void registerDefaultTemplates() {
    // Validation template
    registerTemplate(
        "validation",
        "Validate the following tool execution result:\n\n"
            + "Result: {{result}}\n\n"
            + "Check if the result is correct, complete, and properly formatted. "
            + "Respond with 'VALID' if it passes validation, or 'INVALID' with reasons if it fails.");

    // Correction template
    registerTemplate(
        "correction",
        "The following result failed validation:\n\n"
            + "Original Result: {{result}}\n"
            + "Validation Errors: {{errors}}\n\n"
            + "Please correct the result to address these errors. "
            + "Provide only the corrected result without explanation.");

    // Tool execution template
    registerTemplate(
        "tool_execution",
        "Execute the following tool:\n\n"
            + "Tool: {{toolName}}\n"
            + "Description: {{description}}\n"
            + "Parameters:\n{{parameters}}\n\n"
            + "Provide the result of executing this tool.");

    // Agent planning template
    registerTemplate(
        "agent_planning",
        "Given the following goal and context, create an execution plan:\n\n"
            + "Goal: {{goal}}\n"
            + "Available Tools: {{tools}}\n"
            + "Context: {{context}}\n\n"
            + "Provide a step-by-step plan to achieve the goal using the available tools.");

    // Supervisor review template
    registerTemplate(
        "supervisor_review",
        "Review the following agent execution:\n\n"
            + "Flow ID: {{flowId}}\n"
            + "Agent ID: {{agentId}}\n"
            + "Results: {{results}}\n"
            + "Issues: {{issues}}\n\n"
            + "Determine if the execution should be approved, rejected, or requires further action. "
            + "Provide your decision and reasoning.");

    // Error analysis template
    registerTemplate(
        "error_analysis",
        "Analyze the following error:\n\n"
            + "Error Message: {{errorMessage}}\n"
            + "Error Code: {{errorCode}}\n"
            + "Context: {{context}}\n\n"
            + "Provide a diagnosis of the error and suggest corrective actions.");

    // Result summarization template
    registerTemplate(
        "result_summary",
        "Summarize the following execution results:\n\n"
            + "Results: {{results}}\n"
            + "Execution Time: {{executionTime}}ms\n"
            + "Status: {{status}}\n\n"
            + "Provide a concise summary suitable for end-users.");

    LOG.info("Registered {} default prompt templates", templates.size());
  }

  /**
   * Registers a new prompt template.
   *
   * @param templateId Unique identifier for the template
   * @param templateText The template text with {{variable}} placeholders
   */
  public void registerTemplate(String templateId, String templateText) {
    PromptTemplate template = PromptTemplate.from(templateText);
    templates.put(templateId, template);
    LOG.debug("Registered template: {}", templateId);
  }

  /**
   * Renders a template with the provided variables.
   *
   * @param templateId The template identifier
   * @param variables Map of variable names to values
   * @return The rendered Prompt
   * @throws IllegalArgumentException if template is not found
   */
  public Prompt renderTemplate(String templateId, Map<String, Object> variables) {
    PromptTemplate template = templates.get(templateId);
    if (template == null) {
      throw new IllegalArgumentException("Template not found: " + templateId);
    }

    LOG.debug("Rendering template: {} with {} variables", templateId, variables.size());
    return template.apply(variables);
  }

  /**
   * Renders a template with a single variable.
   *
   * @param templateId The template identifier
   * @param variableName The variable name
   * @param value The variable value
   * @return The rendered Prompt
   */
  public Prompt renderTemplate(String templateId, String variableName, Object value) {
    Map<String, Object> variables = new HashMap<>();
    variables.put(variableName, value);
    return renderTemplate(templateId, variables);
  }

  /**
   * Gets the raw template text.
   *
   * @param templateId The template identifier
   * @return The template text, or null if not found
   */
  public String getTemplateText(String templateId) {
    PromptTemplate template = templates.get(templateId);
    return template != null ? template.template() : null;
  }

  /**
   * Checks if a template exists.
   *
   * @param templateId The template identifier
   * @return true if the template exists
   */
  public boolean hasTemplate(String templateId) {
    return templates.containsKey(templateId);
  }

  /**
   * Removes a template from the registry.
   *
   * @param templateId The template identifier
   * @return true if the template was removed
   */
  public boolean unregisterTemplate(String templateId) {
    boolean removed = templates.remove(templateId) != null;
    if (removed) {
      LOG.debug("Unregistered template: {}", templateId);
    }
    return removed;
  }

  /**
   * Gets all registered template IDs.
   *
   * @return Set of template IDs
   */
  public java.util.Set<String> getTemplateIds() {
    return new java.util.HashSet<>(templates.keySet());
  }

  /**
   * Gets the number of registered templates.
   *
   * @return Template count
   */
  public int getTemplateCount() {
    return templates.size();
  }

  /**
   * Convenience method for rendering validation prompts.
   *
   * @param result The result to validate
   * @return The rendered Prompt
   */
  public Prompt validationPrompt(Object result) {
    return renderTemplate("validation", "result", result);
  }

  /**
   * Convenience method for rendering correction prompts.
   *
   * @param result The original result
   * @param errors The validation errors
   * @return The rendered Prompt
   */
  public Prompt correctionPrompt(Object result, String errors) {
    Map<String, Object> vars = new HashMap<>();
    vars.put("result", result);
    vars.put("errors", errors);
    return renderTemplate("correction", vars);
  }

  /**
   * Convenience method for rendering tool execution prompts.
   *
   * @param toolName The tool name
   * @param description The tool description
   * @param parameters The tool parameters
   * @return The rendered Prompt
   */
  public Prompt toolExecutionPrompt(String toolName, String description, String parameters) {
    Map<String, Object> vars = new HashMap<>();
    vars.put("toolName", toolName);
    vars.put("description", description);
    vars.put("parameters", parameters);
    return renderTemplate("tool_execution", vars);
  }

  /**
   * Convenience method for rendering supervisor review prompts.
   *
   * @param flowId The flow ID
   * @param agentId The agent ID
   * @param results The execution results
   * @param issues Any issues that occurred
   * @return The rendered Prompt
   */
  public Prompt supervisorReviewPrompt(
      String flowId, String agentId, String results, String issues) {
    Map<String, Object> vars = new HashMap<>();
    vars.put("flowId", flowId);
    vars.put("agentId", agentId);
    vars.put("results", results);
    vars.put("issues", issues);
    return renderTemplate("supervisor_review", vars);
  }
}
