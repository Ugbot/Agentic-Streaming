package org.jagentic.core.trace;

/**
 * Minimal tracing SPI — a span per turn, timer fire, and CEP match. The {@link #NOOP} default costs
 * nothing; {@link RecordingTracer} captures spans for tests/inspection; an OpenTelemetry exporter is
 * an opt-in adapter (the heavy dependency stays out of the core). Engine adapters can bridge to their
 * native tracing behind the same SPI.
 */
public interface Tracer {

  /** Begin a span; attributes/events are added via the returned {@link Span}, closed with end(). */
  Span start(String name);

  Tracer NOOP = name -> Span.NOOP;
}
