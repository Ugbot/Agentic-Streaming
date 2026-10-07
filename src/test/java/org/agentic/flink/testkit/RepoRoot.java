package org.agentic.flink.testkit;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;

/**
 * Locates the repository root so tests can load shared fixtures ({@code examples/pipelines/*.yaml})
 * regardless of the working directory Maven, the IDE or a reactor build hands them.
 *
 * <p>The root is the nearest ancestor of {@code basedir} (set by surefire and failsafe), then of
 * {@code user.dir}, then of this class's code location, that contains both {@code reactor/pom.xml}
 * and {@code examples/pipelines}. A fixture that does not exist is an error, never a skip.
 */
public final class RepoRoot {

  private static final Path ROOT = locate();

  private RepoRoot() {}

  /** The repository root directory. */
  public static Path dir() {
    return ROOT;
  }

  /** {@code <repo>/examples/pipelines/<name>}; throws if the file is missing. */
  public static Path examplePipeline(String name) {
    return require(ROOT.resolve("examples").resolve("pipelines").resolve(name));
  }

  /** Returns {@code path} if it is a regular file, otherwise throws naming the absolute path. */
  public static Path require(Path path) {
    if (!Files.isRegularFile(path)) {
      throw new IllegalStateException("required fixture is missing: " + path.toAbsolutePath());
    }
    return path;
  }

  private static Path locate() {
    List<Path> starts = new ArrayList<>();
    String basedir = System.getProperty("basedir");
    if (basedir != null && !basedir.isBlank()) {
      starts.add(Path.of(basedir));
    }
    starts.add(Path.of(System.getProperty("user.dir")));
    CodeSource source = RepoRoot.class.getProtectionDomain().getCodeSource();
    if (source != null && source.getLocation() != null) {
      try {
        starts.add(Path.of(source.getLocation().toURI()));
      } catch (URISyntaxException e) {
        throw new IllegalStateException("unusable code source " + source.getLocation(), e);
      }
    }
    for (Path start : starts) {
      for (Path dir = start.toAbsolutePath().normalize(); dir != null; dir = dir.getParent()) {
        if (Files.isRegularFile(dir.resolve("reactor").resolve("pom.xml"))
            && Files.isDirectory(dir.resolve("examples").resolve("pipelines"))) {
          return dir;
        }
      }
    }
    throw new IllegalStateException(
        "repository root (reactor/pom.xml and examples/pipelines) not found above any of "
            + starts);
  }
}
