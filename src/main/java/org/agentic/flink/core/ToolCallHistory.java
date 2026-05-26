package org.agentic.flink.core;

import java.io.Serializable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ToolCallHistory implements Serializable {

  private String toolId;
  private String toolName;
  private Map<String, Object> input;
  private Object output;
  private boolean success;
  private String errorMessage;
  private Long startTime;
  private Long endTime;
  private Long durationMs;

  public ToolCallHistory(String toolId, String toolName, Map<String, Object> input) {
    this.toolId = toolId;
    this.toolName = toolName;
    this.input = input;
    this.startTime = System.currentTimeMillis();
  }

  public void complete(Object output) {
    this.output = output;
    this.success = true;
    this.endTime = System.currentTimeMillis();
    this.durationMs = this.endTime - this.startTime;
  }

  public void fail(String errorMessage) {
    this.errorMessage = errorMessage;
    this.success = false;
    this.endTime = System.currentTimeMillis();
    this.durationMs = this.endTime - this.startTime;
  }
}
