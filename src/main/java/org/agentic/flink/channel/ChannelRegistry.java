package org.agentic.flink.channel;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Named registry of {@link Channel}s. Lets job-graph builders refer to channels by stable name
 * rather than rebuilding identical specs in several places.
 *
 * <p>The registry is serializable and ships with the operator spec; the channels themselves are
 * built on demand by calling {@link Channel#open}.
 */
public final class ChannelRegistry implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Map<String, Channel<?>> channels;

  private ChannelRegistry(Map<String, Channel<?>> channels) {
    this.channels = Collections.unmodifiableMap(new LinkedHashMap<>(channels));
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<Channel<T>> get(String name) {
    return Optional.ofNullable((Channel<T>) channels.get(name));
  }

  @SuppressWarnings("unchecked")
  public <T> Channel<T> require(String name) {
    Channel<T> c = (Channel<T>) channels.get(name);
    if (c == null) {
      throw new IllegalArgumentException("No channel registered under '" + name + "'");
    }
    return c;
  }

  public Map<String, Channel<?>> all() {
    return channels;
  }

  public int size() {
    return channels.size();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final Map<String, Channel<?>> channels = new LinkedHashMap<>();

    public Builder add(String name, Channel<?> channel) {
      channels.put(name, channel);
      return this;
    }

    public ChannelRegistry build() {
      return new ChannelRegistry(channels);
    }
  }
}
