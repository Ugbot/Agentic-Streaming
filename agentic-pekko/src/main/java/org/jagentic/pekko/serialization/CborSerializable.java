package org.jagentic.pekko.serialization;

/** Marker bound to {@code jackson-cbor} in application.conf — every actor message, event and
 * state that may cross the wire (remoting / sharding) or hit a journal implements this, so we
 * never fall back to Java serialization. */
public interface CborSerializable {}
