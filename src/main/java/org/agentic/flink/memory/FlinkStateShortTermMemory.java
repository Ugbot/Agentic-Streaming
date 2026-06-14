package org.agentic.flink.memory;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * Default {@link ShortTermMemory} implementation backed by Flink keyed state.
 *
 * <p>State layout (per key):
 *
 * <ul>
 *   <li>{@code ValueState<AgentContext> contextState} — the conversation context
 *   <li>{@code MapState<String, ContextItem> itemState} — active items, keyed by item id
 * </ul>
 *
 * <p>If the spec declares a non-zero TTL, both descriptors are configured with {@link
 * StateTtlConfig#cleanupIncrementally(int, boolean)} and {@link
 * StateTtlConfig.UpdateType#OnCreateAndWrite}. Cleanup runs inline with state-backend
 * compaction (RocksDB) or scan (HashMap), so it costs nothing extra beyond a per-entry timestamp.
 */
public final class FlinkStateShortTermMemory implements ShortTermMemory {

  private final ValueState<AgentContext> contextState;
  private final MapState<String, ContextItem> itemState;

  private FlinkStateShortTermMemory(
      ValueState<AgentContext> contextState, MapState<String, ContextItem> itemState) {
    this.contextState = contextState;
    this.itemState = itemState;
  }

  /**
   * Build a spec using the default TTL of zero (i.e. no TTL — state lives until the key is
   * explicitly cleared or the job's checkpoint is dropped).
   */
  public static ShortTermMemorySpec spec() {
    return spec(Duration.ZERO);
  }

  /** Build a spec with the given TTL. {@link Duration#ZERO} disables TTL. */
  public static ShortTermMemorySpec spec(Duration ttl) {
    return new Spec(ttl == null ? Duration.ZERO : ttl);
  }

  @Override
  public Optional<AgentContext> getContext() throws Exception {
    return Optional.ofNullable(contextState.value());
  }

  @Override
  public void putContext(AgentContext context) throws Exception {
    contextState.update(context);
  }

  @Override
  public void putItem(ContextItem item) throws Exception {
    if (item == null || item.getItemId() == null) {
      throw new IllegalArgumentException("ContextItem and itemId must be non-null");
    }
    itemState.put(item.getItemId(), item);
  }

  @Override
  public Optional<ContextItem> getItem(String itemId) throws Exception {
    return Optional.ofNullable(itemState.get(itemId));
  }

  @Override
  public void removeItem(String itemId) throws Exception {
    itemState.remove(itemId);
  }

  @Override
  public List<ContextItem> items() throws Exception {
    List<ContextItem> out = new ArrayList<>();
    for (Map.Entry<String, ContextItem> e : itemState.entries()) {
      out.add(e.getValue());
    }
    return out;
  }

  @Override
  public int size() throws Exception {
    int n = 0;
    for (String ignored : itemState.keys()) {
      n++;
    }
    return n;
  }

  @Override
  public void clearItems() throws Exception {
    itemState.clear();
  }

  @Override
  public int totalTokens() throws Exception {
    int total = 0;
    for (Map.Entry<String, ContextItem> e : itemState.entries()) {
      Integer tc = e.getValue().getTokenCount();
      if (tc != null) {
        total += tc;
      }
    }
    return total;
  }

  /** Serializable spec; constructs descriptors and obtains state in {@link #bind}. */
  static final class Spec implements ShortTermMemorySpec {
    private static final long serialVersionUID = 1L;
    private final Duration ttl;

    Spec(Duration ttl) {
      this.ttl = ttl;
    }

    @Override
    public Duration ttl() {
      return ttl;
    }

    @Override
    public ShortTermMemory bind(RuntimeContext rc) throws Exception {
      ValueStateDescriptor<AgentContext> contextDescriptor =
          new ValueStateDescriptor<>("shortterm.context", AgentContext.class);
      MapStateDescriptor<String, ContextItem> itemDescriptor =
          new MapStateDescriptor<>("shortterm.items", String.class, ContextItem.class);

      if (!ttl.isZero() && !ttl.isNegative()) {
        StateTtlConfig ttlConfig =
            StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .cleanupIncrementally(10, false)
                .build();
        contextDescriptor.enableTimeToLive(ttlConfig);
        itemDescriptor.enableTimeToLive(ttlConfig);
      }

      return new FlinkStateShortTermMemory(
          rc.getState(contextDescriptor), rc.getMapState(itemDescriptor));
    }

    @Override
    public String providerName() {
      return "FlinkStateShortTermMemory(ttl=" + ttl + ")";
    }
  }
}
