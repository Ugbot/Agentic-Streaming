package org.agentic.flink.example.incident;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.agentic.flink.example.incident.IncidentAgentExample.MetricSample;
import org.agentic.flink.example.incident.IncidentAgentExample.ZScoreAnomalyConnection;
import org.agentic.flink.inference.GenericInferenceModel;
import org.agentic.flink.inference.InferenceSetup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The incident showcase only produces output when its synthetic stream yields three
 * consecutive anomalies on one host, because the CEP pattern needs {@code times(3)}
 * within one window. These checks pin the synthetic data to the detector the example
 * wires up, so the example cannot silently degrade into a job that prints nothing.
 */
class IncidentAgentExampleTest {

  private static final int WINDOW = 20;
  private static final double THRESHOLD = 2.5;

  @Test
  @DisplayName("Synthetic metrics end with three consecutive anomalies on the same host")
  void syntheticStreamTriggersThreeAnomalies() {
    List<MetricSample> samples = IncidentAgentExample.synthMetrics();
    GenericInferenceModel detector =
        new ZScoreAnomalyConnection(WINDOW, THRESHOLD).bind(null).asGeneric();
    InferenceSetup setup =
        InferenceSetup.builder().withModelName("z-score").withModelUri("inproc").build();

    List<MetricSample> anomalous = new ArrayList<>();
    for (MetricSample sample : samples) {
      Map<String, Object> out = detector.infer(Map.of("value", sample.value()), setup);
      if (Boolean.TRUE.equals(out.get("anomaly"))) {
        anomalous.add(sample);
      }
    }

    assertEquals(3, anomalous.size(), "expected exactly the three spikes to be anomalous");
    assertEquals(samples.subList(samples.size() - 3, samples.size()), anomalous);
    assertEquals(1, anomalous.stream().map(MetricSample::host).distinct().count());
  }

  @Test
  @DisplayName("Synthetic timestamps are monotonic so event-time watermarks can advance")
  void syntheticTimestampsAreMonotonic() {
    List<MetricSample> samples = IncidentAgentExample.synthMetrics();
    for (int i = 1; i < samples.size(); i++) {
      assertTrue(
          samples.get(i).ts() > samples.get(i - 1).ts(),
          "sample " + i + " is not later than sample " + (i - 1));
    }
    long span = samples.get(samples.size() - 1).ts() - samples.get(samples.size() - 3).ts();
    assertTrue(span < java.time.Duration.ofMinutes(5).toMillis(), "spikes must fit the CEP window");
  }
}
