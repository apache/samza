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
package org.apache.samza.diagnostics;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;


public class TestDiagnosticsStreamMessage {
  private static final String JOB_NAME = "Testjob";
  private static final String JOB_ID = "test job id";
  private static final String CONTAINER_NAME = "sample container name";
  private static final String EXECUTION_ENV_CONTAINER_ID = "exec container id";
  private static final String SAMZA_EPOCH_ID = "epoch-123";
  private static final String TASK_CLASS_VERSION = "0.0.1";
  private static final String SAMZA_VERSION = "1.3.0";
  private static final String HOSTNAME = "sample host name";
  private final long timestamp = System.currentTimeMillis();
  private final long resetTimestamp = System.currentTimeMillis();
  private final Config config = new MapConfig(ImmutableMap.of("job.name", JOB_NAME, "job.id", JOB_ID));

  private DiagnosticsStreamMessage getDiagnosticsStreamMessage(Optional<String> samzaEpochId) {
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        new DiagnosticsStreamMessage(JOB_NAME, JOB_ID, CONTAINER_NAME, EXECUTION_ENV_CONTAINER_ID, samzaEpochId,
            TASK_CLASS_VERSION, SAMZA_VERSION, HOSTNAME, timestamp, resetTimestamp);

    diagnosticsStreamMessage.addContainerMb(1024);
    diagnosticsStreamMessage.addContainerNumCores(2);
    diagnosticsStreamMessage.addNumPersistentStores(3);
    diagnosticsStreamMessage.addConfig(config);

    diagnosticsStreamMessage.addProcessorStopEvents(getProcessorStopEventList());
    return diagnosticsStreamMessage;
  }

  public static Collection<DiagnosticsExceptionEvent> getExceptionList() {
    BoundedList<DiagnosticsExceptionEvent> boundedList = new BoundedList<>("exceptions");
    DiagnosticsExceptionEvent diagnosticsExceptionEvent =
        new DiagnosticsExceptionEvent(1, new Exception("this is a samza exception", new Exception("cause")),
            new HashMap<>());

    boundedList.add(diagnosticsExceptionEvent);
    return boundedList.getValues();
  }

  public List<ProcessorStopEvent> getProcessorStopEventList() {
    List<ProcessorStopEvent> stopEventList = new ArrayList<>();
    stopEventList.add(new ProcessorStopEvent("0", EXECUTION_ENV_CONTAINER_ID, HOSTNAME, 101));
    return stopEventList;
  }

  public static Map<String, ContainerModel> getSampleContainerModels() {
    Map<String, ContainerModel> containerModels = new HashMap<>();
    Map<TaskName, TaskModel> tasks = new HashMap<>();

    Set<SystemStreamPartition> sspsForTask1 = new HashSet<>();
    sspsForTask1.add(new SystemStreamPartition("kafka", "test-stream", new Partition(0)));
    tasks.put(new TaskName("Partition 0"), new TaskModel(new TaskName("Partition 0"), sspsForTask1, new Partition(0)));

    Set<SystemStreamPartition> sspsForTask2 = new HashSet<>();
    sspsForTask2.add(new SystemStreamPartition("kafka", "test-stream", new Partition(1)));
    tasks.put(new TaskName("Partition 1"), new TaskModel(new TaskName("Partition 1"), sspsForTask2, new Partition(1)));

    containerModels.put("0", new ContainerModel("0", tasks));
    return containerModels;
  }

  /**
   * Tests basic operations on {@link DiagnosticsStreamMessage}.
   */
  @Test
  public void basicTest() {
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        getDiagnosticsStreamMessage(Optional.of(SAMZA_EPOCH_ID));
    Collection<DiagnosticsExceptionEvent> exceptionEventList = getExceptionList();
    diagnosticsStreamMessage.addDiagnosticsExceptionEvents(exceptionEventList);
    diagnosticsStreamMessage.addProcessorStopEvents(getProcessorStopEventList());
    diagnosticsStreamMessage.addContainerModels(getSampleContainerModels());

    Assert.assertEquals(1024, (int) diagnosticsStreamMessage.getContainerMb());
    Assert.assertEquals(2, (int) diagnosticsStreamMessage.getContainerNumCores());
    Assert.assertEquals(3, (int) diagnosticsStreamMessage.getNumPersistentStores());
    Assert.assertEquals(config, diagnosticsStreamMessage.getConfig());
    Assert.assertEquals(exceptionEventList, diagnosticsStreamMessage.getExceptionEvents());
    Assert.assertEquals(getSampleContainerModels(), diagnosticsStreamMessage.getContainerModels());
    Assert.assertEquals(diagnosticsStreamMessage.getProcessorStopEvents(), getProcessorStopEventList());
  }

  /**
   * Tests serialization and deserialization of a {@link DiagnosticsStreamMessage}
   */
  @Test
  public void serdeTest() {
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        getDiagnosticsStreamMessage(Optional.of(SAMZA_EPOCH_ID));
    Collection<DiagnosticsExceptionEvent> exceptionEventList = getExceptionList();
    diagnosticsStreamMessage.addDiagnosticsExceptionEvents(exceptionEventList);
    diagnosticsStreamMessage.addProcessorStopEvents(getProcessorStopEventList());
    diagnosticsStreamMessage.addContainerModels(getSampleContainerModels());

    MetricsSnapshot metricsSnapshot = diagnosticsStreamMessage.convertToMetricsSnapshot();
    MetricsHeader expectedHeader = new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXECUTION_ENV_CONTAINER_ID,
        Optional.of(SAMZA_EPOCH_ID), DiagnosticsManager.class.getName(), TASK_CLASS_VERSION, SAMZA_VERSION,
        HOSTNAME, timestamp, resetTimestamp);
    Assert.assertEquals(metricsSnapshot.getHeader(), expectedHeader);

    Map<String, Map<String, Object>> metricsMap = metricsSnapshot.getMetrics().getAsMap();
    Assert.assertTrue(metricsMap.get("org.apache.samza.container.SamzaContainerMetrics").containsKey("exceptions"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("containerModels"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("numPersistentStores"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("containerNumCores"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("containerMemoryMb"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("stopEvents"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("config"));

    DiagnosticsStreamMessage convertedDiagnosticsStreamMessage =
        DiagnosticsStreamMessage.convertToDiagnosticsStreamMessage(metricsSnapshot);
    Assert.assertEquals(convertedDiagnosticsStreamMessage, diagnosticsStreamMessage);
  }

  @Test
  public void testSerdeEmptySamzaEpochIdInHeader() {
    DiagnosticsStreamMessage diagnosticsStreamMessage = getDiagnosticsStreamMessage(Optional.empty());
    MetricsSnapshot metricsSnapshot = diagnosticsStreamMessage.convertToMetricsSnapshot();
    MetricsHeader expectedHeader =
        new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXECUTION_ENV_CONTAINER_ID, Optional.empty(),
            DiagnosticsManager.class.getName(), TASK_CLASS_VERSION, SAMZA_VERSION, HOSTNAME, timestamp, resetTimestamp);
    Assert.assertEquals(metricsSnapshot.getHeader(), expectedHeader);

    DiagnosticsStreamMessage convertedDiagnosticsStreamMessage =
        DiagnosticsStreamMessage.convertToDiagnosticsStreamMessage(metricsSnapshot);
    Assert.assertEquals(convertedDiagnosticsStreamMessage, diagnosticsStreamMessage);
  }
}
