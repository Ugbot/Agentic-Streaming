package org.jagentic.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A secret is named in a document, never written in one: {@code env:NAME} or {@code file:/path}. It is
 * resolved at the moment of the call, so it reaches no document, argument, log or journal entry.
 *
 * <p>A document could name any variable or file the process can read, so a reference must also be on
 * the operator's list: {@code AGENTIC_SECRET_REFS} (or the system property {@code
 * agentic.secret_refs}), comma-separated prefixes such as {@code env:AGENTIC_,file:/run/secrets/}.
 * That is the default. A file's path is normalised before it is compared, so {@code ..} does not get
 * out; a symbolic link inside a listed directory is followed, so do not put one there.
 */
public final class Secrets {

  private Secrets() {}

  /** Thrown when a reference is not one, or names nothing. It never carries a secret's value. */
  public static final class Unresolved extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    Unresolved(String message) {
      super(message);
    }
  }

  /** True for something shaped like a reference: anything else in that place is a literal. */
  public static boolean isReference(String value) {
    return value != null && (value.matches("env:[A-Za-z_][A-Za-z0-9_]*") || value.matches("file:/.+"));
  }

  /** The operator's list when it sets none. */
  public static final String DEFAULT_ALLOWED = "env:AGENTIC_,file:/run/secrets/,file:/var/run/secrets/";

  /** True when the operator's list covers {@code reference}, a reference already. */
  public static boolean allowed(String reference) {
    String listed = System.getProperty("agentic.secret_refs", System.getenv("AGENTIC_SECRET_REFS"));
    String subject = reference;
    if (reference.startsWith("file:")) {
      subject = "file:" + Path.of(reference.substring(5)).normalize();
    }
    for (String prefix : (listed == null || listed.isBlank() ? DEFAULT_ALLOWED : listed).split(",")) {
      String p = prefix.strip();
      if (!p.isEmpty() && subject.startsWith(p)) {
        return true;
      }
    }
    return false;
  }

  /** Why {@code reference} may not be read, for an error: it names the setting, never a value. */
  static String offTheList(String reference) {
    return reference + " is not on the operator's list of secret references (AGENTIC_SECRET_REFS)";
  }

  /** The value of {@code ${NAME}} in a connection link, under the same list as {@code env:NAME}. */
  public static String expand(String value) {
    if (value != null && value.startsWith("${") && value.endsWith("}")) {
      String reference = "env:" + value.substring(2, value.length() - 1);
      if (!isReference(reference) || !allowed(reference)) {
        throw new IllegalArgumentException(offTheList(value));
      }
      String v = System.getenv(reference.substring(4));
      return v == null ? "" : v;
    }
    return value;
  }

  public static String resolve(String reference) {
    if (!isReference(reference)) {
      throw new Unresolved("not a secret reference; use env:NAME or file:/path");
    }
    if (!allowed(reference)) {
      throw new Unresolved(offTheList(reference));
    }
    if (reference.startsWith("env:")) {
      String value = System.getProperty("agentic.secret." + reference.substring(4), System.getenv(reference.substring(4)));
      if (value == null || value.isEmpty()) {
        throw new Unresolved(reference + " is not set");
      }
      return value;
    }
    try {
      return Files.readString(Path.of(reference.substring(5)), StandardCharsets.UTF_8).strip();
    } catch (IOException e) {
      throw new Unresolved(reference + " cannot be read");
    }
  }
}
