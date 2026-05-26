package org.agentic.flink.tools;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractToolExecutor implements ToolExecutor {

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractToolExecutor.class);

  protected final String toolId;
  protected final String description;

  protected AbstractToolExecutor(String toolId, String description) {
    this.toolId = toolId;
    this.description = description;
  }

  @Override
  public String getToolId() {
    return toolId;
  }

  @Override
  public String getDescription() {
    return description;
  }

  protected void logExecution(Map<String, Object> parameters) {
    LOG.info("Executing tool: {} with parameters: {}", toolId, parameters);
  }

  protected <T> T getRequiredParameter(Map<String, Object> parameters, String key, Class<T> type) {
    Object value = parameters.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Required parameter missing: " + key);
    }
    if (!type.isInstance(value)) {
      throw new IllegalArgumentException(
          "Parameter " + key + " must be of type " + type.getSimpleName());
    }
    return type.cast(value);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getOptionalParameter(
      Map<String, Object> parameters, String key, Class<T> type, T defaultValue) {
    Object value = parameters.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (!type.isInstance(value)) {
      return defaultValue;
    }
    return (T) value;
  }
}
