package org.jagentic.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A secret is named in a document, never written in one: {@code env:NAME} or {@code file:/path}. It is
 * resolved at the moment of the call, so it reaches no document, argument, log or journal entry.
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

  public static String resolve(String reference) {
    if (!isReference(reference)) {
      throw new Unresolved("not a secret reference; use env:NAME or file:/path");
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
