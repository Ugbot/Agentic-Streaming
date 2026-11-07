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

  /**
   * A field is a credential when one of these is a whole word of its name, so {@code access_token},
   * {@code client_secret}, {@code x-api-key} and {@code refreshToken} are, and {@code max_tokens} is not.
   */
  private static final Set<String> SECRET_FIELDS = Set.of("password", "passwd", "passphrase", "secret", "token",
      "api_key", "apikey", "authorization", "credential", "credentials", "private_key", "cookie");
  private static final Pattern CAMEL = Pattern.compile("([a-z0-9])([A-Z])");
  private static final Pattern SEPARATORS = Pattern.compile("[^a-z0-9]+");
  private static final List<Pattern> SECRET_SHAPES = List.of(
      Pattern.compile("(?i)\\bbearer\\s+[a-z0-9._~+/=-]{8,}"),
      Pattern.compile("\\bsk-[A-Za-z0-9_-]{16,}"),
      Pattern.compile("\\bAKIA[0-9A-Z]{16}\\b"),
      Pattern.compile("\\beyJ[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}"),
      Pattern.compile("\\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})"),
      Pattern.compile("\\bxox[abprs]-[A-Za-z0-9-]{10,}"),
      Pattern.compile("\\b[sr]k_(?:live|test)_[A-Za-z0-9]{16,}"),
      Pattern.compile("\\bAIza[0-9A-Za-z_-]{35}"));

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
    return isSecretField(field) || fields.contains(field.toLowerCase(Locale.ROOT));
  }

  static boolean isSecretField(String field) {
    String split = CAMEL.matcher(field).replaceAll("$1_$2").toLowerCase(Locale.ROOT);
    String name = "_" + SEPARATORS.matcher(split).replaceAll("_").replaceAll("^_+|_+$", "") + "_";
    for (String f : SECRET_FIELDS) {
      if (name.contains("_" + f + "_")) {
        return true;
      }
    }
    return false;
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
