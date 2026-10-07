package org.agentic.flink.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RepoRootTest {

  @Test
  void rootContainsTheReactorAndTheSharedPipelines() {
    Path root = RepoRoot.dir();
    assertTrue(Files.isRegularFile(root.resolve("reactor").resolve("pom.xml")), root.toString());
    assertTrue(Files.isDirectory(root.resolve("examples").resolve("pipelines")), root.toString());
    assertTrue(root.isAbsolute());
  }

  @Test
  void examplePipelineResolvesAnExistingFixture() {
    Path yaml = RepoRoot.examplePipeline("banking.yaml");
    assertTrue(Files.isRegularFile(yaml), yaml.toString());
    assertEquals(RepoRoot.dir().resolve("examples/pipelines/banking.yaml"), yaml);
  }

  @Test
  void missingFixtureIsAnErrorNotASkip() {
    String name = "missing-" + UUID.randomUUID() + ".yaml";
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> RepoRoot.examplePipeline(name));
    assertTrue(e.getMessage().contains(name), e.getMessage());
  }
}
