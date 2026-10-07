package org.agentic.flink.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Every public top-level type compiled from {@code src/main} carries exactly one of {@link Public},
 * {@link Experimental} or {@link Internal}. The check runs over the compiled classes so it covers
 * whatever the active Maven profiles include (for example {@code plugins/flintagents} with {@code
 * -P flink-agents}).
 */
class ApiStabilityMarkerTest {

  private static final List<Class<? extends Annotation>> MARKERS =
      List.of(Public.class, Experimental.class, Internal.class);

  @Test
  void everyPublicTopLevelTypeHasExactlyOneMarker() throws IOException, URISyntaxException {
    Path classes =
        Path.of(Public.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    assertTrue(Files.isDirectory(classes), "main classes directory: " + classes);

    List<String> unmarked = new ArrayList<>();
    List<String> overMarked = new ArrayList<>();
    int checked = 0;
    try (Stream<Path> files = Files.walk(classes)) {
      for (Path file : (Iterable<Path>) files::iterator) {
        String rel = classes.relativize(file).toString();
        if (!rel.endsWith(".class")) continue;
        String name = rel.substring(0, rel.length() - ".class".length()).replace('/', '.');
        String simple = name.substring(name.lastIndexOf('.') + 1);
        if (simple.contains("$") || simple.equals("package-info") || simple.equals("module-info")) {
          continue;
        }
        Class<?> type = load(name);
        if (!Modifier.isPublic(type.getModifiers()) || type.getEnclosingClass() != null) continue;
        checked++;
        long count = markerCount(type);
        if (count == 0) unmarked.add(name);
        if (count > 1) overMarked.add(name);
      }
    }

    assertTrue(checked > 0, "found no public top-level types under " + classes);
    assertEquals(
        List.of(),
        unmarked,
        "public top-level types without a stability marker (@Public, @Experimental or @Internal)");
    assertEquals(List.of(), overMarked, "public top-level types with more than one marker");
  }

  @Test
  void markerCountSeesMissingAndDuplicateMarkers() {
    assertEquals(0, markerCount(Unmarked.class));
    assertEquals(1, markerCount(OnceMarked.class));
    assertEquals(2, markerCount(TwiceMarked.class));
  }

  @Test
  void markersAreDocumentedRuntimeTypeAnnotations() {
    for (Class<? extends Annotation> marker : MARKERS) {
      assertTrue(marker.isAnnotationPresent(Documented.class), marker + " @Documented");
      Retention retention = marker.getAnnotation(Retention.class);
      assertTrue(retention != null, marker + " @Retention");
      assertEquals(RetentionPolicy.RUNTIME, retention.value(), marker.toString());
      Target target = marker.getAnnotation(Target.class);
      assertTrue(target != null, marker + " @Target");
      assertEquals(List.of(ElementType.TYPE), List.of(target.value()), marker.toString());
      assertEquals(1, markerCount(marker), marker.toString());
    }
  }

  static long markerCount(Class<?> type) {
    return MARKERS.stream().filter(type::isAnnotationPresent).count();
  }

  static class Unmarked {}

  @Experimental
  static class OnceMarked {}

  @Public
  @Internal
  static class TwiceMarked {}

  private static Class<?> load(String name) {
    try {
      return Class.forName(name, false, ApiStabilityMarkerTest.class.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      throw new AssertionError("cannot load " + name, e);
    }
  }
}
