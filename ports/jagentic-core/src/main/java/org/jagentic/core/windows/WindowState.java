package org.jagentic.core.windows;

/** A numeric aggregate over a set of events: how many, and their summed value. */
public record WindowState(int count, double sum) {}
