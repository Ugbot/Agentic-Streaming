package org.agentic.flink.operator;

import org.agentic.flink.control.DebugEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

/**
 * Constants shared across the agentic-flink debug pipeline. {@link #SIDE_OUT} is the single side
 * output every framework operator publishes to; the wiring helper unions all such side outputs
 * into one stream and sinks it to the configured debug channel.
 */
public final class OperatorDebug {
  private OperatorDebug() {}

  /** Side-output tag every framework operator publishes debug events to. */
  public static final OutputTag<DebugEvent> SIDE_OUT =
      new OutputTag<>("agentic-debug", TypeInformation.of(new TypeHint<DebugEvent>() {}));
}
