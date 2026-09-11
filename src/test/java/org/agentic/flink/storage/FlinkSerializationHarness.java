package org.agentic.flink.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.SerializableFunction;

/**
 * Pushes a storage provider through the serialization paths Flink really uses and hands the
 * deserialized copy back to the test:
 *
 * <ul>
 *   <li>{@link #viaJobGraph}: the provider is a field of a {@link RichMapFunction} in a job that
 *       runs on a local MiniCluster. Flink closure-cleans, serializes the operator into the job
 *       graph, deserializes it on the task thread and calls {@code open()}; the function then uses
 *       the store and emits the result.
 *   <li>{@link #viaTypeSerializer}: the provider is written and read back through the {@link
 *       TypeSerializer} Flink derives for its class (the generic/Kryo route used for state and
 *       stream records).
 *   <li>{@link #viaInstantiationUtil}: {@link InstantiationUtil}, the primitive underneath both.
 * </ul>
 */
public final class FlinkSerializationHarness {

  private FlinkSerializationHarness() {}

  public static <S extends Serializable> List<String> viaJobGraph(
      S store, SerializableFunction<S, String> useStore, List<String> inputs) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
    List<String> out = new ArrayList<>();
    try (CloseableIterator<String> it =
        env.fromData(inputs, Types.STRING)
            .map(new UseStore<>(store, useStore))
            .returns(Types.STRING)
            .executeAndCollect()) {
      it.forEachRemaining(out::add);
    }
    return out;
  }

  @SuppressWarnings("unchecked")
  public static <S extends Serializable> S viaTypeSerializer(S store) throws Exception {
    TypeSerializer<S> serializer =
        ((TypeInformation<S>) TypeInformation.of(store.getClass()))
            .createSerializer(new SerializerConfigImpl());
    DataOutputSerializer out = new DataOutputSerializer(4096);
    serializer.serialize(store, out);
    return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
  }

  public static <S extends Serializable> S viaInstantiationUtil(S store) throws Exception {
    byte[] bytes = InstantiationUtil.serializeObject(store);
    return InstantiationUtil.deserializeObject(bytes, store.getClass().getClassLoader());
  }

  private static final class UseStore<S extends Serializable>
      extends RichMapFunction<String, String> {
    private static final long serialVersionUID = 1L;
    private final S store;
    private final SerializableFunction<S, String> useStore;

    UseStore(S store, SerializableFunction<S, String> useStore) {
      this.store = store;
      this.useStore = useStore;
    }

    @Override
    public void open(OpenContext openContext) {
      if (store == null) {
        throw new IllegalStateException("store field was not restored from the job graph");
      }
    }

    @Override
    public String map(String input) {
      return input + "=" + useStore.apply(store);
    }
  }
}
