package org.jagentic.core.trace;

/** A single trace span — attach key/value attributes and named events, then {@link #end()}. */
public interface Span {

  Span attr(String key, String value);

  Span event(String name);

  void end();

  Span NOOP = new Span() {
    @Override
    public Span attr(String key, String value) {
      return this;
    }

    @Override
    public Span event(String name) {
      return this;
    }

    @Override
    public void end() {
      // no-op
    }
  };
}
