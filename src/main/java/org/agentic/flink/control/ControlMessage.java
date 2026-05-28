package org.agentic.flink.control;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

/**
 * Envelope for the agentic-flink runtime control plane. Carried on the broadcast input that
 * every framework operator subscribes to; lets the user (or another job) flip per-operator
 * behaviour at runtime.
 *
 * <p>Sealed so concrete message types are enumerated for both polymorphic JSON via Jackson and
 * exhaustive {@code switch}-on-{@code instanceof} dispatch in operator code. Add a new control
 * type by (1) declaring a record that {@code implements ControlMessage}, (2) adding it to
 * {@code permits}, and (3) registering it as a {@link JsonSubTypes} entry below — operators
 * that don't recognise the type ignore it.
 *
 * <p>Two universal fields:
 *
 * <ul>
 *   <li>{@link #operatorId()} — stable name of the target operator (e.g. {@code
 *       "L4.feature-aggregator"}). Use {@code "*"} to target every operator in the job.
 *   <li>{@link #ttlMillis()} — how long the directive stays in effect. {@code 0} = framework
 *       default (typically 120 000 ms = 2 min), {@code -1} = permanent until explicitly
 *       cancelled, positive = explicit TTL.
 * </ul>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = DebugControl.class, name = "debug")})
public sealed interface ControlMessage extends Serializable permits DebugControl {

  /** Stable operator name, or {@code "*"} to broadcast to every operator. */
  String operatorId();

  /** How long the directive stays in effect; see class javadoc for sentinel values. */
  long ttlMillis();

  /** Sentinel: when an envelope carries {@code ttlMillis == 0}, use the framework default. */
  long DEFAULT_TTL_MILLIS = 120_000L;

  /** Sentinel: permanent directive — never expires. */
  long PERMANENT = -1L;

  /** Sentinel: broadcast to every operator in the job. */
  String ALL_OPERATORS = "*";
}
