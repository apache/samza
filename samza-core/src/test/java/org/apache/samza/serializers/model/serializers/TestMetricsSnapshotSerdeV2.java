package org.apache.samza.serializers.model.serializers;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.diagnostics.DiagnosticsExceptionEvent;
import org.apache.samza.metrics.ListGauge;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.junit.Assert;
import org.junit.Test;


public class TestMetricsSnapshotSerdeV2 {

  @Test
  public void testSerde() {
    MetricsHeader metricsHeader =
        new MetricsHeader("jobName", "i001", "container 0", "source", "300.14.25.1", "1", "1", 1, 1);

    ListGauge listGauge = new ListGauge<DiagnosticsExceptionEvent>("exceptions");
    DiagnosticsExceptionEvent diagnosticsExceptionEvent =
        new DiagnosticsExceptionEvent(1, new SamzaException("this is a samza exception", new RuntimeException("cause")),
            new HashMap());
    listGauge.add(diagnosticsExceptionEvent);

    String samzaContainerMetricsGroupName = "org.apache.samza.container.SamzaContainerMetrics";
    Map<String, Map<String, Object>> metricMessage = new HashMap<>();
    metricMessage.put(samzaContainerMetricsGroupName, new HashMap<>());
    metricMessage.get(samzaContainerMetricsGroupName).put("exceptions", listGauge.getValues());
    metricMessage.get(samzaContainerMetricsGroupName).put("commit-calls", 0);

    MetricsSnapshot metricsSnapshot = new MetricsSnapshot(metricsHeader, new Metrics(metricMessage));

    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    byte[] serializedBytes = metricsSnapshotSerde.toBytes(metricsSnapshot);

    MetricsSnapshot deserializedMetricsSnapshot = metricsSnapshotSerde.fromBytes(serializedBytes);

    Assert.assertTrue("Headers map should be equal",
        metricsSnapshot.getHeader().getAsMap().equals(deserializedMetricsSnapshot.getHeader().getAsMap()));

    Assert.assertTrue("Metrics map should be equal",
        metricsSnapshot.getMetrics().getAsMap().equals(deserializedMetricsSnapshot.getMetrics().getAsMap()));
  }
}
