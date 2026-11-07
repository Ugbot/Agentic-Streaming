package org.jagentic.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Takes what must not be recorded out of a value, before it reaches the log or the journal. Credentials
 * are always covered, by field name and by shape; {@code policies.redact} adds a workflow's own fields
 * and patterns, such as personal data. A pattern finds what it describes and nothing else, so this
 * narrows what is recorded and does not promise it is clean.
 */
public final class Redactor {

  public static final String MARK = "[redacted]";

  private static final Set<String> SECRET_FIELDS = Set.of("password", "passwd", "secret", "token", "api_key",
      "apikey", "authorization", "credential", "credentials", "private_key");
  private static final List<Pattern> SECRET_SHAPES = List.of(
      Pattern.compile("(?i)\\bbearer\\s+[a-z0-9._~+/=-]{8,}"),
      Pattern.compile("\\bsk-[A-Za-z0-9_-]{16,}"),
      Pattern.compile("\\bAKIA[0-9A-Z]{16}\\b"),
      Pattern.compile("\\beyJ[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}"));

  public static final Redactor DEFAULT = new Redactor(Set.of(), List.of());

  private final Set<String> fields;
  private final List<Pattern> patterns;

  private Redactor(Set<String> fields, List<Pattern> patterns) {
    this.fields = fields;
    this.patterns = patterns;
  }

  /** Reads {@code policies.redact}. */
  public static Redactor fromPolicies(Map<String, Object> policies) {
    if (policies == null || !(policies.get("redact") instanceof Map<?, ?> block)) {
      return DEFAULT;
    }
    Set<String> fields = new java.util.HashSet<>();
    if (block.get("fields") instanceof List<?> listed) {
      listed.forEach(f -> fields.add(String.valueOf(f).toLowerCase(Locale.ROOT)));
    }
    List<Pattern> patterns = new ArrayList<>();
    if (block.get("patterns") instanceof List<?> listed) {
      listed.forEach(p -> patterns.add(Pattern.compile(String.valueOf(p))));
    }
    return new Redactor(fields, patterns);
  }

  /** A copy of {@code value} with what must not be recorded taken out. */
  @SuppressWarnings("unchecked")
  public <T> T apply(T value) {
    return (T) walk(value);
  }

  private Object walk(Object value) {
    if (value instanceof String s) {
      return text(s);
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> out = new LinkedHashMap<>();
      map.forEach((k, v) -> out.put(k, named(String.valueOf(k)) ? MARK : walk(v)));
      return out;
    }
    if (value instanceof List<?> list) {
      List<Object> out = new ArrayList<>(list.size());
      list.forEach(v -> out.add(walk(v)));
      return out;
    }
    return value;
  }

  private boolean named(String field) {
    String name = field.toLowerCase(Locale.ROOT);
    return SECRET_FIELDS.contains(name) || fields.contains(name);
  }

  private String text(String s) {
    String out = s;
    for (Pattern p : SECRET_SHAPES) {
      out = p.matcher(out).replaceAll(MARK);
    }
    for (Pattern p : patterns) {
      out = p.matcher(out).replaceAll(MARK);
    }
    return out;
  }
}
