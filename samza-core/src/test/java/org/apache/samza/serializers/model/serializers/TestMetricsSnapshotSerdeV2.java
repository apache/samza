/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
        new MetricsHeader("jobName", "i001", "container 0", "test container id", "source", "300.14.25.1", "1", "1", 1,
            1);

    ListGauge listGauge = new ListGauge<DiagnosticsExceptionEvent>("exceptions");
    DiagnosticsExceptionEvent diagnosticsExceptionEvent1 =
        new DiagnosticsExceptionEvent(1, new SamzaException("this is a samza exception", new RuntimeException("cause")),
            new HashMap());

    listGauge.add(diagnosticsExceptionEvent1);

    String samzaContainerMetricsGroupName = "org.apache.samza.container.SamzaContainerMetrics";
    Map<String, Map<String, Object>> metricMessage = new HashMap<>();
    metricMessage.put(samzaContainerMetricsGroupName, new HashMap<>());
    metricMessage.get(samzaContainerMetricsGroupName).put("exceptions", listGauge.getValues());
    metricMessage.get(samzaContainerMetricsGroupName).put("commit-calls", 0);

    MetricsSnapshot metricsSnapshot = new MetricsSnapshot(metricsHeader, new Metrics(metricMessage));
    byte[] serializedBytes = new MetricsSnapshotSerdeV2().toBytes(metricsSnapshot);

    MetricsSnapshot deserializedMetricsSnapshot = new MetricsSnapshotSerdeV2().fromBytes(serializedBytes);

    Assert.assertTrue("Headers map should be equal",
        metricsSnapshot.getHeader().getAsMap().equals(deserializedMetricsSnapshot.getHeader().getAsMap()));
  }
}
