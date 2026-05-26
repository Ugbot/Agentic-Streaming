package org.agentic.flink.channel;

import org.agentic.flink.context.core.ContextItem;
import java.io.Serializable;
import java.util.Objects;

/** A {@link ContextItem} carrying its target flow-id, as emitted by memory-feed channels. */
public final class KeyedContextItem implements Serializable {
  private static final long serialVersionUID = 1L;

  private String flowId;
  private ContextItem item;

  public KeyedContextItem() {}

  public KeyedContextItem(String flowId, ContextItem item) {
    this.flowId = Objects.requireNonNull(flowId, "flowId");
    this.item = Objects.requireNonNull(item, "item");
  }

  public String getFlowId() {
    return flowId;
  }

  public void setFlowId(String flowId) {
    this.flowId = flowId;
  }

  public ContextItem getItem() {
    return item;
  }

  public void setItem(ContextItem item) {
    this.item = item;
  }
}
