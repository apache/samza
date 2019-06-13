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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.diagnostics.BoundedList;
import org.apache.samza.diagnostics.DiagnosticsExceptionEvent;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;


public class TestMetricsSnapshotSerdeV2 {

  @Test
  public void testSerdeOfMessagesWithoutContainerModel() throws IOException {
    MetricsHeader metricsHeader =
        new MetricsHeader("jobName", "i001", "container 0", "test container ID", "source", "300.14.25.1", "1", "1",
            System.currentTimeMillis(), System.currentTimeMillis());

    BoundedList boundedList = new BoundedList<DiagnosticsExceptionEvent>("exceptions");
    DiagnosticsExceptionEvent diagnosticsExceptionEvent1 =
        new DiagnosticsExceptionEvent(1, new SamzaException("this is a samza exception", new RuntimeException("cause")),
            new HashMap());

    boundedList.add(diagnosticsExceptionEvent1);

    String samzaContainerMetricsGroupName = "org.apache.samza.container.SamzaContainerMetrics";
    Map<String, Map<String, Object>> metricMessage = new HashMap<>();
    metricMessage.put(samzaContainerMetricsGroupName, new HashMap<>());
    metricMessage.get(samzaContainerMetricsGroupName).put("exceptions", boundedList.getValues());
    metricMessage.get(samzaContainerMetricsGroupName).put("commit-calls", 0);
    MetricsSnapshot metricsSnapshot = new MetricsSnapshot(metricsHeader, new Metrics(metricMessage));

    ObjectMapper test = new ObjectMapper();
    test.enableDefaultTyping(ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE);
    byte[] testBytes = test.writeValueAsString(convertMap(metricsSnapshot.getAsMap())).getBytes("UTF-8");
    MetricsSnapshot testSnapshot = new MetricsSnapshotSerdeV2().fromBytes(testBytes);

    Assert.assertTrue(testSnapshot.getHeader().getAsMap().equals(metricsSnapshot.getHeader().getAsMap()));
    Assert.assertTrue(testSnapshot.getMetrics().getAsMap().equals(metricsSnapshot.getMetrics().getAsMap()));
  }

  @Test
  public void testSerde() {
    MetricsHeader metricsHeader =
        new MetricsHeader("jobName", "i001", "container 0", "test container ID", "source", "300.14.25.1", "1", "1",
            System.currentTimeMillis(), System.currentTimeMillis());

    BoundedList boundedList = new BoundedList<DiagnosticsExceptionEvent>("exceptions");
    DiagnosticsExceptionEvent diagnosticsExceptionEvent1 =
        new DiagnosticsExceptionEvent(1, new SamzaException("this is a samza exception", new RuntimeException("cause")),
            new HashMap());

    boundedList.add(diagnosticsExceptionEvent1);

    String samzaContainerMetricsGroupName = "org.apache.samza.container.SamzaContainerMetrics";
    Map<String, Map<String, Object>> metricMessage = new HashMap<>();
    metricMessage.put(samzaContainerMetricsGroupName, new HashMap<>());
    metricMessage.get(samzaContainerMetricsGroupName).put("exceptions", boundedList.getValues());
    metricMessage.get(samzaContainerMetricsGroupName).put("commit-calls", 0);

    String diagnosticsManagerGroupName = "org.apache.samza.diagnostics.DiagnosticsManager";
    metricMessage.put(diagnosticsManagerGroupName, new HashMap<>());
    metricMessage.get(diagnosticsManagerGroupName).put("containerCount", 1);
    metricMessage.get(diagnosticsManagerGroupName).put("containerMemoryMb", 1024);
    metricMessage.get(diagnosticsManagerGroupName).put("containerNumCores", 2);
    metricMessage.get(diagnosticsManagerGroupName).put("numStores", 5);

    Map<String, ContainerModel> containerModels = new HashMap<>();
    Map<TaskName, TaskModel> tasks = new HashMap<>();

    Set<SystemStreamPartition> sspsForTask1 = new HashSet<>();
    sspsForTask1.add(new SystemStreamPartition("kafka", "test-stream", new Partition(0)));
    tasks.put(new TaskName("Partition 0"), new TaskModel(new TaskName("Partition 0"), sspsForTask1, new Partition(0)));

    Set<SystemStreamPartition> sspsForTask2 = new HashSet<>();
    sspsForTask2.add(new SystemStreamPartition("kafka", "test-stream", new Partition(1)));
    tasks.put(new TaskName("Partition 1"), new TaskModel(new TaskName("Partition 1"), sspsForTask2, new Partition(1)));

    containerModels.put("0", new ContainerModel("0", tasks));

    metricMessage.get(diagnosticsManagerGroupName).put("containerModels", containerModels);

    MetricsSnapshot metricsSnapshot = new MetricsSnapshot(metricsHeader, new Metrics(metricMessage));
    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    byte[] serializedBytes = metricsSnapshotSerde.toBytes(metricsSnapshot);

    MetricsSnapshot deserializedMetricsSnapshot = metricsSnapshotSerde.fromBytes(serializedBytes);

    Assert.assertTrue(metricsSnapshot.getHeader().getAsMap().equals(deserializedMetricsSnapshot.getHeader().getAsMap()));
  }

  HashMap convertMap(Map<String, Object> map) {
    HashMap retVal = new HashMap(map);
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() instanceof Map) {
        retVal.put(entry.getKey(), convertMap((Map<String, Object>) entry.getValue()));
      }
    }
    return retVal;
  }
}
