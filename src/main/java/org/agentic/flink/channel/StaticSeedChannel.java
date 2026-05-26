package org.agentic.flink.channel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Channel that emits a fixed list of elements. Useful for seeding the crawler frontier with
 * starting URLs and for tests where a controlled input stream is needed.
 *
 * <p>Wraps {@link StreamExecutionEnvironment#fromCollection(java.util.Collection,
 * TypeInformation)} so the source preserves the supplied {@link TypeInformation} for downstream
 * keyBy / state operations.
 */
public final class StaticSeedChannel<T> implements Channel<T> {
  private static final long serialVersionUID = 1L;

  private final ArrayList<T> seeds;
  private final TypeInformation<T> typeInfo;

  public StaticSeedChannel(Collection<T> seeds, TypeInformation<T> typeInfo) {
    if (seeds == null) {
      throw new IllegalArgumentException("seeds must be non-null");
    }
    this.seeds = new ArrayList<>(seeds);
    this.typeInfo = typeInfo;
  }

  /** Construct from a varargs list; infers type from the first element's class. */
  @SafeVarargs
  public StaticSeedChannel(T... seeds) {
    this(seeds == null ? List.of() : List.of(seeds), inferType(seeds));
  }

  @SuppressWarnings("unchecked")
  private static <T> TypeInformation<T> inferType(T[] seeds) {
    if (seeds == null || seeds.length == 0) {
      throw new IllegalArgumentException("Cannot infer type from empty seed list");
    }
    return (TypeInformation<T>) TypeInformation.of(seeds[0].getClass());
  }

  @Override
  public DataStream<T> open(StreamExecutionEnvironment env) {
    return env.fromCollection(seeds, typeInfo).name("static-seed[" + seeds.size() + "]");
  }

  @Override
  public TypeInformation<T> elementType() {
    return typeInfo;
  }

  @Override
  public String providerName() {
    return "static-seed";
  }
}
