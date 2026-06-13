package org.agentic.flink.example.banking;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.embedding.HashEmbeddingConnection;
import org.agentic.flink.example.banking.env.EnvApiClient;
import org.agentic.flink.example.banking.env.EnvApiToolExecutor;
import org.agentic.flink.example.banking.env.ListEnvToolsExecutor;
import org.agentic.flink.example.banking.safety.AuthorizationToolGuard;
import org.agentic.flink.example.banking.safety.SessionAuthState;
import org.agentic.flink.tools.ToolExecutor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every tool a routed-graph path operator ({@link
 * org.agentic.flink.example.banking.graph.BankingPathFunction}) holds is shipped <b>inside the Flink
 * operator</b>, so it must be Java-serializable — otherwise the job dies at submit with a
 * {@code NotSerializableException} from {@code ClosureCleaner}. This test round-trips each tool to
 * guard that contract (it caught {@code KbSearchTool$Doc} / {@code VectorKbSearchTool} holding
 * non-serializable index state). Uses randomized temp KB docs.
 */
final class BankingToolSerializationTest {

  @TempDir Path kbDir;
  @TempDir Path cacheDir;

  private void writeDoc(String id) throws Exception {
    String json =
        "{\"id\":\"" + id + "\",\"title\":\"Title " + id + "\",\"content\":\"content "
            + UUID.randomUUID() + "\"}";
    Files.writeString(kbDir.resolve(id + ".json"), json);
  }

  @SuppressWarnings("unchecked")
  private static <T> T roundTrip(T tool) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(tool);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return (T) ois.readObject();
    }
  }

  @Test
  @DisplayName("KbSearchTool round-trips (its loaded docs are serializable)")
  void keywordKbSerializable() throws Exception {
    for (int i = 0; i < 4; i++) {
      writeDoc("kw-" + i);
    }
    KbSearchTool tool = KbSearchTool.fromDirectory(kbDir.toString());
    ToolExecutor restored = roundTrip(tool);
    assertInstanceOf(KbSearchTool.class, restored);
    assertNotNull(restored.execute(java.util.Map.of("query", "content")).join());
  }

  @Test
  @DisplayName("VectorKbSearchTool round-trips (config serialized; index is transient/lazy)")
  void vectorKbSerializable() throws Exception {
    for (int i = 0; i < 5; i++) {
      writeDoc("vec-" + i);
    }
    VectorKbSearchTool tool =
        VectorKbSearchTool.build(
            kbDir.toString(),
            cacheDir.toString(),
            new HashEmbeddingConnection(),
            EmbeddingSetup.of("hash", 256, true));
    VectorKbSearchTool restored = roundTrip(tool);
    assertNotNull(restored);
    // The restored tool must lazily rebuild its index on the task side and still search.
    assertNotNull(restored.execute(java.util.Map.of("query", "content", "top_k", 3)).join());
  }

  @Test
  @DisplayName("Env tools + authorization guard round-trip (transient HttpClient rebuilt lazily)")
  void envToolsSerializable() throws Exception {
    EnvApiClient client = new EnvApiClient("http://localhost:1/", "tok-" + UUID.randomUUID(), 1000);
    assertInstanceOf(EnvApiToolExecutor.class, roundTrip(EnvApiToolExecutor.fallback(client)));
    assertInstanceOf(ListEnvToolsExecutor.class, roundTrip(new ListEnvToolsExecutor(client)));
    AuthorizationToolGuard guard =
        new AuthorizationToolGuard(
            EnvApiToolExecutor.fallback(client), false, false, new SessionAuthState());
    assertInstanceOf(AuthorizationToolGuard.class, roundTrip(guard));
  }
}
