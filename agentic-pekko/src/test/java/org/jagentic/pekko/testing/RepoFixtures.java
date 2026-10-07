package org.jagentic.pekko.testing;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Resolves the shared {@code examples/pipelines/*.yaml} fixtures from the repository root, found
 * by walking up from {@code basedir} (set by surefire) or {@code user.dir} until a directory holds
 * both {@code reactor/pom.xml} and {@code examples/pipelines}. A missing fixture throws; tests
 * never skip because the working directory differs.
 */
public final class RepoFixtures {

  private static final Path ROOT = locate();

  private RepoFixtures() {}

  public static Path examplePipeline(String name) {
    Path yaml = ROOT.resolve("examples").resolve("pipelines").resolve(name);
    if (!Files.isRegularFile(yaml)) {
      throw new IllegalStateException("required fixture is missing: " + yaml);
    }
    return yaml;
  }

  private static Path locate() {
    String basedir = System.getProperty("basedir");
    Path[] starts =
        basedir == null || basedir.isBlank()
            ? new Path[] {Path.of(System.getProperty("user.dir"))}
            : new Path[] {Path.of(basedir), Path.of(System.getProperty("user.dir"))};
    for (Path start : starts) {
      for (Path dir = start.toAbsolutePath().normalize(); dir != null; dir = dir.getParent()) {
        if (Files.isRegularFile(dir.resolve("reactor").resolve("pom.xml"))
            && Files.isDirectory(dir.resolve("examples").resolve("pipelines"))) {
          return dir;
        }
      }
    }
    throw new IllegalStateException(
        "repository root (reactor/pom.xml and examples/pipelines) not found above "
            + Arrays.toString(starts));
  }
}
