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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.diagnostics.BoundedList;
import org.apache.samza.diagnostics.DiagnosticsExceptionEvent;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class TestMetricsSnapshotSerdeV2 {
  @Test
  public void testSerializeAndDeserialize() {
    SamzaException samzaException = new SamzaException("this is a samza exception", new RuntimeException("cause"));
    MetricsSnapshot metricsSnapshot = metricsSnapshot(samzaException);
    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    byte[] serializedBytes = metricsSnapshotSerde.toBytes(metricsSnapshot);
    MetricsSnapshot deserializedMetricsSnapshot = metricsSnapshotSerde.fromBytes(serializedBytes);
    assertEquals(metricsSnapshot, deserializedMetricsSnapshot);
  }

  @Test
  public void testSerialize() {
    SamzaException samzaException = new SamzaException("this is a samza exception", new RuntimeException("cause"));
    MetricsSnapshot metricsSnapshot = metricsSnapshot(samzaException);
    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    byte[] serializedBytes = metricsSnapshotSerde.toBytes(metricsSnapshot);
    assertArrayEquals(expectedSeralizedSnapshot(samzaException).getBytes(StandardCharsets.UTF_8), serializedBytes);
  }

  @Test
  public void testDeserialize() {
    SamzaException samzaException = new SamzaException("this is a samza exception", new RuntimeException("cause"));
    MetricsSnapshot expectedSnapshot = metricsSnapshot(samzaException);
    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    MetricsSnapshot deserializedSnapshot =
        metricsSnapshotSerde.fromBytes(expectedSeralizedSnapshot(samzaException).getBytes(StandardCharsets.UTF_8));
    assertEquals(expectedSnapshot, deserializedSnapshot);
  }

  private static MetricsSnapshot metricsSnapshot(Exception exception) {
    MetricsHeader metricsHeader =
        new MetricsHeader("jobName", "i001", "container 0", "test container ID", "source", "300.14.25.1", "1", "1", 1,
            1);
    BoundedList<DiagnosticsExceptionEvent> boundedList = new BoundedList<>("exceptions");
    DiagnosticsExceptionEvent diagnosticsExceptionEvent = new DiagnosticsExceptionEvent(1, exception, new HashMap<>());
    boundedList.add(diagnosticsExceptionEvent);
    Map<String, Map<String, Object>> metricMessage = new HashMap<>();
    Map<String, Object> samzaContainerMetrics = new HashMap<>();
    samzaContainerMetrics.put("commit-calls", 0);
    metricMessage.put("org.apache.samza.container.SamzaContainerMetrics", samzaContainerMetrics);
    Map<String, Object> exceptions = new HashMap<>();
    exceptions.put("exceptions", boundedList.getValues());
    metricMessage.put("org.apache.samza.exceptions", exceptions);
    return new MetricsSnapshot(metricsHeader, new Metrics(metricMessage));
  }

  private static String expectedSeralizedSnapshot(Exception exception) {
    String stackTrace = ExceptionUtils.getStackTrace(exception);
    // in serialized string, backslash in whitespace characters (e.g. \n, \t) are escaped
    String escapedStackTrace = stackTrace.replace("\n", "\\n").replace("\t", "\\t");
    return
        "{\"header\":[\"java.util.HashMap\",{\"job-id\":\"i001\",\"exec-env-container-id\":\"test container ID\",\"samza-version\":\"1\",\"job-name\":\"jobName\",\"host\":\"1\",\"reset-time\":[\"java.lang.Long\",1],\"container-name\":\"container 0\",\"source\":\"source\",\"time\":[\"java.lang.Long\",1],\"version\":\"300.14.25.1\"}],\"metrics\":[\"java.util.HashMap\",{\"org.apache.samza.exceptions\":[\"java.util.HashMap\",{\"exceptions\":[\"java.util.Collections$UnmodifiableRandomAccessList\",[[\"org.apache.samza.diagnostics.DiagnosticsExceptionEvent\",{\"timestamp\":1,\"exceptionType\":\"org.apache.samza.SamzaException\",\"exceptionMessage\":\"this is a samza exception\",\"compactExceptionStackTrace\":\""
            + escapedStackTrace
            + "\",\"mdcMap\":[\"java.util.HashMap\",{}]}]]]}],\"org.apache.samza.container.SamzaContainerMetrics\":[\"java.util.HashMap\",{\"commit-calls\":0}]}]}";
  }
}
