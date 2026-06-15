package org.jagentic.core.cep;

import java.util.List;
import java.util.Map;

import org.jagentic.core.Event;

/** A completed pattern match: the matched events in order, plus a name→event map (stage name to the
 * event that satisfied it) — the portable form of Flink's {@code Map<String, List<Event>>} match. */
public record Match(List<Event> events, Map<String, Event> named) {}
