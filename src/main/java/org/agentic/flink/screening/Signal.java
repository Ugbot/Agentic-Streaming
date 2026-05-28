package org.agentic.flink.screening;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * A detector firing: which detector, which {@link Phase}, how much it adds to combined risk, and why.
 *
 * <p>Declared as a Flink-compatible POJO (public no-arg constructor, JavaBean getters/setters)
 * so the type rides through Flink's {@code PojoSerializer} without falling back to Kryo (which
 * can't construct records with their final-field canonical ctor). Record-style accessors are
 * preserved for source-compat with existing callers.
 */
public final class Signal implements Serializable {
  private static final long serialVersionUID = 1L;

  private String detector;
  private Phase phase;
  private double weight;
  private String reason;

  /** Required by Flink's POJO serializer. */
  public Signal() {}

  @JsonCreator
  public Signal(
      @JsonProperty("detector") String detector,
      @JsonProperty("phase") Phase phase,
      @JsonProperty("weight") double weight,
      @JsonProperty("reason") String reason) {
    this.detector = detector;
    this.phase = phase;
    this.weight = weight;
    this.reason = reason;
  }

  // ---- record-style accessors (used throughout the screening package) ----

  public String detector() {
    return detector;
  }

  public Phase phase() {
    return phase;
  }

  public double weight() {
    return weight;
  }

  public String reason() {
    return reason;
  }

  // ---- JavaBean getters/setters (required by Flink POJO detection) ----

  public String getDetector() {
    return detector;
  }

  public void setDetector(String detector) {
    this.detector = detector;
  }

  public Phase getPhase() {
    return phase;
  }

  public void setPhase(Phase phase) {
    this.phase = phase;
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Signal that)) {
      return false;
    }
    return Double.compare(weight, that.weight) == 0
        && Objects.equals(detector, that.detector)
        && phase == that.phase
        && Objects.equals(reason, that.reason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(detector, phase, weight, reason);
  }

  @Override
  public String toString() {
    return "Signal[detector="
        + detector
        + ", phase="
        + phase
        + ", weight="
        + weight
        + ", reason="
        + reason
        + ']';
  }
}
