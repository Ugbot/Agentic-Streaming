package org.jagentic.core.trace;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A tracer that records completed spans in end() order — for tests and local inspection. */
public final class RecordingTracer implements Tracer {

  public record Recorded(String name, Map<String, String> attrs, List<String> events) {}

  private final List<Recorded> spans = new ArrayList<>();

  @Override
  public Span start(String name) {
    Map<String, String> attrs = new LinkedHashMap<>();
    List<String> events = new ArrayList<>();
    return new Span() {
      @Override
      public Span attr(String key, String value) {
        attrs.put(key, value);
        return this;
      }

      @Override
      public Span event(String evt) {
        events.add(evt);
        return this;
      }

      @Override
      public void end() {
        synchronized (spans) {
          spans.add(new Recorded(name, attrs, events));
        }
      }
    };
  }

  public synchronized List<Recorded> spans() {
    synchronized (spans) {
      return List.copyOf(spans);
    }
  }

  /** Span names in end() order. */
  public List<String> names() {
    return spans().stream().map(Recorded::name).toList();
  }
}
