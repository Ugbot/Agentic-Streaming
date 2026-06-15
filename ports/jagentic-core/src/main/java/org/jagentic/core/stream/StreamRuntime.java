package org.jagentic.core.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.TurnResult;
import org.jagentic.core.timers.Timer;
import org.jagentic.core.timers.TimerService;
import org.jagentic.core.trace.Span;
import org.jagentic.core.trace.Tracer;

/**
 * Drives a {@link Channel} of events through a backend {@link Runtime} as agent turns — the portable
 * realization of the project's thesis that an agent is a materialized view over a stream of events.
 * {@code Runtime.submit(event)} becomes the one-shot sugar; this is the streaming form.
 *
 * <p>Per-conversation ordering is the runtime's concern (the {@code LocalRuntime} per-key lock, a
 * Kafka partition, a Pekko sharded entity). {@link EventObserver}s see each event before it becomes a
 * turn — the seam CEP matchers, window aggregators, and tracers plug into in later phases.</p>
 */
public final class StreamRuntime {

  private final Runtime runtime;
  private final List<EventObserver> observers = new CopyOnWriteArrayList<>();
  private Tracer tracer = Tracer.NOOP;

  public StreamRuntime(Runtime runtime) {
    this.runtime = runtime;
  }

  /** Register an observer of the raw event stream (chainable). */
  public StreamRuntime observe(EventObserver observer) {
    observers.add(observer);
    return this;
  }

  /** Trace turns and timer fires through this tracer (chainable; default no-op). */
  public StreamRuntime withTracer(Tracer tracer) {
    this.tracer = tracer == null ? Tracer.NOOP : tracer;
    return this;
  }

  private TurnResult submitTraced(String spanName, Event event) {
    for (EventObserver observer : observers) {
      observer.onEvent(event);
    }
    Span span = tracer.start(spanName);
    span.attr("conversation", event.conversationId());
    TurnResult r = runtime.submit(event);
    span.attr("path", r.path).attr("ok", Boolean.toString(r.ok));
    for (String tool : r.toolCalls) {
      span.event("tool:" + tool);
    }
    span.end();
    return r;
  }

  /** Drain every currently-available event from the channel as a turn, in arrival order, and return
   * the results. Observers see each event first. Returns when the channel next reports empty. */
  public List<TurnResult> run(Channel<Event> channel) {
    List<TurnResult> results = new ArrayList<>();
    for (Optional<Event> next = channel.poll(); next.isPresent(); next = channel.poll()) {
      results.add(submitTraced("turn", next.get()));
    }
    return results;
  }

  /** Fire every timer due at {@code now}, submitting each timer's payload as a turn (observers see it
   * first), in deadline order. The streaming counterpart of an SLA / escalate-after-N firing. */
  public List<TurnResult> fireDueTimers(TimerService timers, long now) {
    List<TurnResult> results = new ArrayList<>();
    for (Timer timer : timers.advanceTo(now)) {
      results.add(submitTraced("timer.fire", timer.payload()));
    }
    return results;
  }
}
