package org.jagentic.core;

/**
 * The backend-agnostic seam: process one turn and return its result. {@link LocalRuntime}
 * implements it in-process; engine adapters (Pulsar/Temporal/Kafka-Streams/…) implement
 * it over their runtime. A {@code Backends} factory picks one by name so "choose a
 * backend and the rest falls into place" holds on the JVM too.
 */
public interface Runtime {
  TurnResult submit(Event event);
}
