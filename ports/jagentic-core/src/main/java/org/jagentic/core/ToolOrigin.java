package org.jagentic.core;

/** What asked for a tool. A timer and a CEP pattern run outside any path, so no path's tools cover them. */
public enum ToolOrigin {
  BRAIN, SAGA, COMPENSATION, TIMER, CEP;

  public String wire() {
    return name().toLowerCase();
  }

  public boolean unattended() {
    return this == TIMER || this == CEP;
  }
}
