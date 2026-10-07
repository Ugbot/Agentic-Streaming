package org.agentic.flink.a2a.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The serializer snapshot of {@link A2AJsonTypeInfo} must restore a serializer that can read. */
final class A2AJsonSerializerSnapshotTest {

  @Test
  @DisplayName("a snapshot written and read back restores a serializer for the same element class")
  void snapshotRoundTripRestoresTypedSerializer() throws IOException {
    TypeSerializer<A2ARequest> serializer =
        A2AJsonTypeInfo.of(A2ARequest.class).createSerializer(null);
    A2ARequest request =
        new A2ARequest(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            "agent-" + UUID.randomUUID(),
            A2AMessage.userText(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
            true,
            null,
            Map.of("sub", UUID.randomUUID().toString()));

    DataOutputSerializer out = new DataOutputSerializer(256);
    TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration());
    serializer.serialize(request, out);

    DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
    TypeSerializerSnapshot<A2ARequest> restored =
        TypeSerializerSnapshot.readVersionedSnapshot(in, getClass().getClassLoader());
    TypeSerializer<A2ARequest> restoredSerializer = restored.restoreSerializer();
    assertEquals(serializer, restoredSerializer);

    A2ARequest read = restoredSerializer.deserialize(in);
    assertEquals(request.getTaskId(), read.getTaskId());
    assertEquals(request.getContextId(), read.getContextId());
    assertEquals(request.getMessage().textContent(), read.getMessage().textContent());
    assertEquals(request.getClaims(), read.getClaims());
    assertTrue(
        serializer.snapshotConfiguration().resolveSchemaCompatibility(restored).isCompatibleAsIs());
  }

  @Test
  @DisplayName("a snapshot for a different element class is incompatible")
  void differentElementClassIsIncompatible() {
    TypeSerializerSnapshot<A2AResponse> response =
        A2AJsonTypeInfo.of(A2AResponse.class).createSerializer(null).snapshotConfiguration();
    @SuppressWarnings({"unchecked", "rawtypes"})
    TypeSerializerSnapshot<A2AResponse> request =
        (TypeSerializerSnapshot)
            A2AJsonTypeInfo.of(A2ARequest.class).createSerializer(null).snapshotConfiguration();
    assertTrue(response.resolveSchemaCompatibility(request).isIncompatible());
    assertFalse(response.resolveSchemaCompatibility(response).isIncompatible());
    A2AResponse value =
        A2AResponse.completed(
            UUID.randomUUID().toString(),
            null,
            List.of(A2AArtifact.text(UUID.randomUUID().toString(), "r", "x")));
    assertEquals(A2AResponse.class, response.restoreSerializer().copy(value).getClass());
  }

  @Test
  @DisplayName("snapshots written before the element class was recorded are rejected explicitly")
  void legacySnapshotWithoutTypeIsRejected() {
    A2AJsonTypeInfo.A2AJsonSerializerSnapshot<A2ARequest> snapshot =
        new A2AJsonTypeInfo.A2AJsonSerializerSnapshot<>();
    IOException e =
        assertThrows(
            IOException.class,
            () ->
                snapshot.readSnapshot(
                    3, new DataInputDeserializer(new byte[0]), getClass().getClassLoader()));
    assertTrue(e.getMessage().contains("version 3"));
    assertThrows(NullPointerException.class, () -> new A2AJsonTypeInfo.A2AJsonSerializer<>(null));
  }
}
