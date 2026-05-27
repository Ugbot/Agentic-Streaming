package org.agentic.flink.screening;

import java.io.Serializable;

/**
 * A detector firing: which detector, which {@link Phase}, how much it adds to combined risk, and why.
 *
 * @param detector detector name
 * @param phase the phase that produced it
 * @param weight contribution to combined risk (>= 0)
 * @param reason human-readable explanation
 */
public record Signal(String detector, Phase phase, double weight, String reason)
    implements Serializable {}
