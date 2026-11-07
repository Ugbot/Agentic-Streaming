package org.agentic.flink.typeinfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The Jackson-backed {@link JsonTypeInfo} serializer round-trips a value through Flink's {@link
 * TypeSerializer} contract, and deep-copies a mutable type rather than aliasing it on {@code copy}.
 */
class JsonTypeInfoTest {

  private static <T> T roundTrip(JsonTypeInfo<T> info, T value) throws Exception {
    TypeSerializer<T> ser = info.createSerializer(new SerializerConfigImpl());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ser.serialize(value, new DataOutputViewStreamWrapper(bos));
    return ser.deserialize(
        new DataInputViewStreamWrapper(new ByteArrayInputStream(bos.toByteArray())));
  }

  /** A mutable value with a nested map, the shape that object reuse could alias. */
  public static final class Note {
    public String id;
    public Map<String, Object> data = new LinkedHashMap<>();
  }

  @Test
  @DisplayName("a mutable type round-trips its Map<String,Object> and deep-copies")
  void mutableTypeRoundTripsAndCopies() throws Exception {
    String id = UUID.randomUUID().toString();
    Note note = new Note();
    note.id = id;
    note.data.put("k", "v");
    note.data.put("n", 7);

    JsonTypeInfo<Note> info = JsonTypeInfo.of(Note.class, true);
    Note restored = roundTrip(info, note);
    assertEquals(id, restored.id);
    assertEquals("v", restored.data.get("k"));
    assertEquals(7, ((Number) restored.data.get("n")).intValue());

    // Mutable copy() must deep-copy so Flink object reuse can't alias live state.
    TypeSerializer<Note> ser = info.createSerializer(new SerializerConfigImpl());
    assertFalse(ser.isImmutableType());
    Note copy = ser.copy(note);
    assertNotSame(note, copy);
    copy.data.put("k", "mutated");
    assertEquals("v", note.data.get("k"), "deep copy must not share the data map");
  }
}
