package org.agentic.flink.typeinfo;

import java.lang.reflect.Type;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Base {@link TypeInfoFactory} that hands a type to {@link JsonTypeInfo}. Attach a 3-line subclass
 * to a value class with {@link org.apache.flink.api.common.typeinfo.TypeInfo @TypeInfo} and Flink's
 * {@code TypeExtractor} will serialize that type as JSON (not Kryo) everywhere it flows — stream
 * elements and keyed state alike.
 *
 * <pre>{@code
 * @TypeInfo(AgentEvent.Factory.class)
 * public class AgentEvent { ...
 *   public static final class Factory extends JsonTypeInfoFactory<AgentEvent> {
 *     public Factory() { super(AgentEvent.class, true); }  // mutable
 *   }
 * }
 * }</pre>
 *
 * @param <T> the annotated value type
 */
public abstract class JsonTypeInfoFactory<T> extends TypeInfoFactory<T> {

  private final Class<T> type;
  private final boolean mutable;

  protected JsonTypeInfoFactory(Class<T> type, boolean mutable) {
    this.type = type;
    this.mutable = mutable;
  }

  @Override
  public TypeInformation<T> createTypeInfo(
      Type t, Map<String, TypeInformation<?>> genericParameters) {
    return JsonTypeInfo.of(type, mutable);
  }
}
