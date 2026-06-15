package org.jagentic.core.cep;

import java.util.ArrayList;
import java.util.List;

/**
 * A CEP pattern: an ordered list of named stages with a contiguity each, plus an optional
 * {@code within} time bound on the whole match. The portable counterpart of Flink's
 * {@code Pattern.begin(..).next(..).followedBy(..).within(..)}.
 *
 * <pre>
 *   Pattern p = Pattern.begin("first", Condition.any())
 *       .followedBy("second", Condition.any())
 *       .followedBy("third", Condition.any())
 *       .within(Duration.ofMinutes(5).toMillis());
 * </pre>
 *
 * Contiguity (on the transition INTO a stage): {@code NEXT} = strict (the very next event must match,
 * else the partial is dropped); {@code FOLLOWED_BY} = relaxed (non-matching events are skipped, the
 * partial waits). {@code BEGIN} marks the first stage.
 */
public final class Pattern {

  public enum Contiguity { BEGIN, NEXT, FOLLOWED_BY }

  public record Stage(String name, Contiguity contiguity, Condition condition) {}

  private final List<Stage> stages = new ArrayList<>();
  private long withinMillis = 0; // 0 = unbounded

  private Pattern() {}

  public static Pattern begin(String name, Condition condition) {
    Pattern p = new Pattern();
    p.stages.add(new Stage(name, Contiguity.BEGIN, condition));
    return p;
  }

  /** Strict contiguity: the immediately-next event must satisfy {@code condition}. */
  public Pattern next(String name, Condition condition) {
    stages.add(new Stage(name, Contiguity.NEXT, condition));
    return this;
  }

  /** Relaxed contiguity: skip non-matching events until one satisfies {@code condition}. */
  public Pattern followedBy(String name, Condition condition) {
    stages.add(new Stage(name, Contiguity.FOLLOWED_BY, condition));
    return this;
  }

  /** Bound the whole match to {@code millis} from the first matched event (0 = unbounded). */
  public Pattern within(long millis) {
    this.withinMillis = millis;
    return this;
  }

  public List<Stage> stages() {
    return stages;
  }

  public long withinMillis() {
    return withinMillis;
  }
}
