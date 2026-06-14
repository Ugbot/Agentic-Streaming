package org.agentic.flink.graph;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

/**
 * Reusable wiring for a routed multi-operator agent graph: a stream of already-routed turns is
 * fanned out to a <b>separate keyed operator per path</b>, then their outputs are merged and run
 * through a single verifier operator. Hides the per-path filter + union boilerplate so an agent can
 * be expressed as "a router, some path operators, and a verifier" instead of hand-wired Flink.
 *
 * <pre>{@code
 *   DataStream<Turn> routed = requests.keyBy(key).process(routerFn);     // router sets turn.path
 *   DataStream<Resp> out = RoutedAgentGraph.wire(
 *       routed, Turn::path, Turn::contextId, pathFns, verifierFn, turnType, respType);
 * }</pre>
 *
 * <p>Routing is by a {@code path} field on the mid-stream element (the {@code pathOf} selector),
 * mirroring the tier-filter routing in {@link org.agentic.flink.job.AgentJobGenerator}. Each path
 * operator and the verifier are keyed by the same key (e.g. A2A {@code contextId}), so per-session
 * state (phase, routing budget) is consistent across the graph.
 *
 * @param <K> key type (e.g. contextId)
 * @param <MID> routed turn type carrying a path label
 * @param <OUT> response type
 */
public final class RoutedAgentGraph {

  private RoutedAgentGraph() {}

  /**
   * Fan {@code routed} out to {@code pathFns} by {@code pathOf}, then merge and verify.
   *
   * @param routed turns already tagged with a path (by an upstream router operator)
   * @param pathOf extracts the path label from a turn
   * @param key keyed-state key selector (shared by all path operators + the verifier)
   * @param pathFns path label → the keyed operator handling that path
   * @param verifierFn merges path outputs and produces the response
   */
  public static <K, MID, OUT> DataStream<OUT> wire(
      DataStream<MID> routed,
      KeySelector<MID, String> pathOf,
      KeySelector<MID, K> key,
      Map<String, KeyedProcessFunction<K, MID, MID>> pathFns,
      KeyedProcessFunction<K, MID, OUT> verifierFn,
      TypeInformation<MID> midType,
      TypeInformation<OUT> outType) {

    DataStream<MID> merged = null;
    for (Map.Entry<String, KeyedProcessFunction<K, MID, MID>> e : pathFns.entrySet()) {
      String path = e.getKey();
      DataStream<MID> branch =
          routed
              .filter(turn -> path.equals(safeKey(pathOf, turn)))
              .name("route:" + path)
              .keyBy(key)
              .process(e.getValue())
              .name("path:" + path)
              .returns(midType);
      merged = (merged == null) ? branch : merged.union(branch);
    }
    if (merged == null) {
      throw new IllegalArgumentException("RoutedAgentGraph requires at least one path");
    }
    return merged.keyBy(key).process(verifierFn).name("verifier").returns(outType);
  }

  /** Convenience overload taking the path names explicitly (validates the pathFns cover them). */
  public static <K, MID, OUT> DataStream<OUT> wire(
      DataStream<MID> routed,
      List<String> paths,
      KeySelector<MID, String> pathOf,
      KeySelector<MID, K> key,
      Map<String, KeyedProcessFunction<K, MID, MID>> pathFns,
      KeyedProcessFunction<K, MID, OUT> verifierFn,
      TypeInformation<MID> midType,
      TypeInformation<OUT> outType) {
    for (String p : paths) {
      if (!pathFns.containsKey(p)) {
        throw new IllegalArgumentException("No path operator registered for path '" + p + "'");
      }
    }
    return wire(routed, pathOf, key, pathFns, verifierFn, midType, outType);
  }

  private static <MID> String safeKey(KeySelector<MID, String> pathOf, MID turn) {
    try {
      return pathOf.getKey(turn);
    } catch (Exception e) {
      return null;
    }
  }
}
