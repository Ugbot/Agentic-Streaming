package org.agentic.flink.example;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.djl.repository.zoo.ModelZoo;
import java.net.URI;
import java.util.stream.Stream;
import org.agentic.flink.example.rag.RagResearchExample;
import org.agentic.flink.example.research.LiveResearchExample;
import org.agentic.flink.example.triage.SupportTriageExample;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The showcase examples refer to Hugging Face models through DJL's {@code djl://} scheme.
 * DJL resolves such a URI against the model index of the named zoo, so a model that exists
 * on Hugging Face but is absent from the zoo index fails only at job start with "Invalid djl
 * URL". This test resolves every example reranker URI against the index bundled in the DJL
 * tokenizers jar, in DJL offline mode so no network is involved.
 */
class ExampleDjlModelUrisTest {

  private static String previousOffline;

  @BeforeAll
  static void forceOffline() {
    previousOffline = System.getProperty("offline");
    System.setProperty("offline", "true");
  }

  @AfterAll
  static void restoreOffline() {
    if (previousOffline == null) {
      System.clearProperty("offline");
    } else {
      System.setProperty("offline", previousOffline);
    }
  }

  static Stream<String> rerankerUris() {
    return Stream.of(
        RagResearchExample.RERANKER_MODEL_URI,
        LiveResearchExample.RERANKER_MODEL_URI,
        SupportTriageExample.RERANKER_MODEL_URI);
  }

  @ParameterizedTest
  @MethodSource("rerankerUris")
  @DisplayName("Example reranker URIs resolve to an artifact of the DJL Hugging Face zoo")
  void rerankerUriIsInZooIndex(String uriString) {
    URI uri = URI.create(uriString);
    assertTrue("djl".equals(uri.getScheme()), "expected a djl:// URI: " + uriString);
    ModelZoo zoo = ModelZoo.getModelZoo(uri.getHost());
    assertNotNull(zoo, "model zoo not on the classpath: " + uri.getHost());
    String artifactId = uri.getPath().substring(1);
    assertNotNull(
        zoo.getModelLoader(artifactId),
        () -> artifactId + " is not in the " + uri.getHost() + " zoo index");
  }
}
