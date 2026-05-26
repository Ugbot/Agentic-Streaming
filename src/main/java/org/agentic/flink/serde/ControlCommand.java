package org.agentic.flink.serde;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ControlCommand implements Serializable {

  private String commandId;
  private String key; // userId:chatId or agentId or flowId
  private ControlCommandType type;
  private Map<String, Object> params;
  private Long timestamp;
  private Long ttl; // Time to live in milliseconds (optional)

  public ControlCommand(String key, ControlCommandType type) {
    this.commandId = java.util.UUID.randomUUID().toString();
    this.key = key;
    this.type = type;
    this.params = new HashMap<>();
    this.timestamp = System.currentTimeMillis();
  }

  public void putParam(String paramKey, Object value) {
    if (this.params == null) {
      this.params = new HashMap<>();
    }
    this.params.put(paramKey, value);
  }

  public Object getParam(String paramKey) {
    return this.params != null ? this.params.get(paramKey) : null;
  }

  public <T> T getParam(String paramKey, Class<T> type) {
    Object value = getParam(paramKey);
    if (value != null && type.isInstance(value)) {
      return type.cast(value);
    }
    return null;
  }

  public boolean isExpired() {
    if (this.ttl == null || this.ttl <= 0) {
      return false;
    }
    return System.currentTimeMillis() > (this.timestamp + this.ttl);
  }
}
