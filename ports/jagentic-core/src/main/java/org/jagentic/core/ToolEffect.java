package org.jagentic.core;

/** The values of a tool's {@code effect} (spec section 10): what a repeated call does. */
public final class ToolEffect {
  public static final String READ = "read";
  /** The default: the tool is sent a key and may be called again under it. */
  public static final String IDEMPOTENT = "idempotent";
  /** Never called twice by the runtime: a call with no recorded result waits for a person. */
  public static final String NON_IDEMPOTENT = "non_idempotent";

  private ToolEffect() {}
}
