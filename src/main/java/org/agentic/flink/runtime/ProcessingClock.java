package org.agentic.flink.runtime;

import java.io.Serializable;
import org.apache.flink.streaming.api.TimerService;

/**
 * Where {@link WorkflowTurnFunction} reads the spec's processing-time clock for workflow
 * {@code timers} (section 8 of {@code spec/v1/primitives.md}). The default is the operator's own
 * processing time ({@link TimerService#currentProcessingTime()}); a fixture harness substitutes a
 * clock it advances by hand, since the spec's logical clock only moves when a fixture says so.
 * Implementations are serialized with the operator, so they must carry no live state of their own:
 * a manual clock keeps its reading in a registry the whole JVM shares.
 */
@FunctionalInterface
public interface ProcessingClock extends Serializable {

  /** The current processing-time reading in milliseconds. */
  long nowMs(TimerService timerService);

  /** The operator's processing time: wall clock on the task manager. */
  static ProcessingClock flink() {
    return new FlinkProcessingClock();
  }

  /** Named class rather than a lambda so savepoints restore it by a stable class name. */
  final class FlinkProcessingClock implements ProcessingClock {
    private static final long serialVersionUID = 1L;

    @Override
    public long nowMs(TimerService timerService) {
      return timerService.currentProcessingTime();
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof FlinkProcessingClock;
    }

    @Override
    public int hashCode() {
      return FlinkProcessingClock.class.hashCode();
    }
  }
}
