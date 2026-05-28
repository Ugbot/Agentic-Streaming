package org.agentic.flink.control;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One emission from an operator's debug side-output. Operators only push these when their debug
 * flag is currently on; the framework's predefined debug sink consumes the union of all
 * operators' side outputs.
 *
 * <p>Declared as a Flink-compatible POJO (public no-arg constructor, JavaBean getters/setters)
 * so the framework's debug stream avoids the Kryo fallback path.
 */
public final class DebugEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  private String operatorId;
  private long ts;
  private String kind;
  private Map<String, Object> payload = new HashMap<>();

  /** Required by Flink's POJO serializer. */
  public DebugEvent() {}

  @JsonCreator
  public DebugEvent(
      @JsonProperty("operatorId") String operatorId,
      @JsonProperty("ts") long ts,
      @JsonProperty("kind") String kind,
      @JsonProperty("payload") Map<String, Object> payload) {
    this.operatorId = operatorId;
    this.ts = ts;
    this.kind = kind;
    this.payload = payload == null ? new HashMap<>() : new HashMap<>(payload);
  }

  // ---- accessors (record-style) ----

  public String operatorId() {
    return operatorId;
  }

  public long ts() {
    return ts;
  }

  public String kind() {
    return kind;
  }

  public Map<String, Object> payload() {
    return payload;
  }

  // ---- POJO-shaped getters/setters ----

  public String getOperatorId() {
    return operatorId;
  }

  public void setOperatorId(String operatorId) {
    this.operatorId = operatorId;
  }

  public long getTs() {
    return ts;
  }

  public void setTs(long ts) {
    this.ts = ts;
  }

  public String getKind() {
    return kind;
  }

  public void setKind(String kind) {
    this.kind = kind;
  }

  public Map<String, Object> getPayload() {
    return payload;
  }

  public void setPayload(Map<String, Object> payload) {
    this.payload = payload == null ? new HashMap<>() : payload;
  }

  // ---- canonical equality/hash/string ----

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DebugEvent that)) {
      return false;
    }
    return ts == that.ts
        && Objects.equals(operatorId, that.operatorId)
        && Objects.equals(kind, that.kind)
        && Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operatorId, ts, kind, payload);
  }

  @Override
  public String toString() {
    return "DebugEvent["
        + "operatorId="
        + operatorId
        + ", ts="
        + ts
        + ", kind="
        + kind
        + ", payload="
        + payload
        + ']';
  }
}
