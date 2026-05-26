package org.agentic.flink.serde;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ToolAllowlistUpdate implements Serializable {

  private String userId;
  private String agentId;
  private ToolAllowlistAction action;
  private Set<String> tools;
  private Long timestamp;

  public ToolAllowlistUpdate(String userId, String agentId, ToolAllowlistAction action) {
    this.userId = userId;
    this.agentId = agentId;
    this.action = action;
    this.tools = new HashSet<>();
    this.timestamp = System.currentTimeMillis();
  }

  public void addTool(String toolId) {
    if (this.tools == null) {
      this.tools = new HashSet<>();
    }
    this.tools.add(toolId);
  }

  public boolean isGlobal() {
    return "*".equals(this.userId) || "*".equals(this.agentId);
  }
}
