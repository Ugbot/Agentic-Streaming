package org.jagentic.core.inference;

import java.util.Map;

/** A label + its confidence in [0,1], with the full per-label score map. */
public record Classification(String label, double score, Map<String, Double> scores) {}
