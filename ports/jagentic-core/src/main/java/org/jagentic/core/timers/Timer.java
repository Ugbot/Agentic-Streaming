package org.jagentic.core.timers;

import org.jagentic.core.Event;

/** A scheduled event: when logical/processing time reaches {@code fireAt}, {@code payload} is fired
 * back into the runtime as a turn. {@code id} is unique (re-scheduling the same id replaces). */
public record Timer(String id, long fireAt, Event payload) {}
