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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;


public class TestDiagnosticsStreamMessage {

  private final String jobName = "Testjob";
  private final String jobId = "test job id";
  private final String containerName = "sample container name";
  private final String executionEnvContainerId = "exec container id";
  private final String taskClassVersion = "0.0.1";
  private final String samzaVersion = "1.3.0";
  private final String hostname = "sample host name";
  private final long timestamp = System.currentTimeMillis();
  private final long resetTimestamp = System.currentTimeMillis();

  private DiagnosticsStreamMessage getDiagnosticsStreamMessage() {
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        new DiagnosticsStreamMessage(jobName, jobId, containerName, executionEnvContainerId, taskClassVersion,
            samzaVersion, hostname, timestamp, resetTimestamp);

    diagnosticsStreamMessage.addContainerMb(1024);
    diagnosticsStreamMessage.addContainerNumCores(2);
    diagnosticsStreamMessage.addNumPersistentStores(3);

    diagnosticsStreamMessage.addProcessorStopEvents(getProcessorStopEventList());
    return diagnosticsStreamMessage;
  }

  public static Collection<DiagnosticsExceptionEvent> getExceptionList() {
    BoundedList boundedList = new BoundedList<DiagnosticsExceptionEvent>("exceptions");
    DiagnosticsExceptionEvent diagnosticsExceptionEvent =
        new DiagnosticsExceptionEvent(1, new Exception("this is a samza exception", new Exception("cause")),
            new HashMap());

    boundedList.add(diagnosticsExceptionEvent);
    return boundedList.getValues();
  }

  public List<ProcessorStopEvent> getProcessorStopEventList() {
    List<ProcessorStopEvent> stopEventList = new ArrayList<>();
    stopEventList.add(new ProcessorStopEvent("0", executionEnvContainerId, hostname, 101));
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

    DiagnosticsStreamMessage diagnosticsStreamMessage = getDiagnosticsStreamMessage();
    Collection<DiagnosticsExceptionEvent> exceptionEventList = getExceptionList();
    diagnosticsStreamMessage.addDiagnosticsExceptionEvents(exceptionEventList);
    diagnosticsStreamMessage.addProcessorStopEvents(getProcessorStopEventList());
    diagnosticsStreamMessage.addContainerModels(getSampleContainerModels());

    Assert.assertEquals(1024, (int) diagnosticsStreamMessage.getContainerMb());
    Assert.assertEquals(2, (int) diagnosticsStreamMessage.getContainerNumCores());
    Assert.assertEquals(3, (int) diagnosticsStreamMessage.getNumPersistentStores());
    Assert.assertEquals(exceptionEventList, diagnosticsStreamMessage.getExceptionEvents());
    Assert.assertEquals(getSampleContainerModels(), diagnosticsStreamMessage.getContainerModels());
    Assert.assertEquals(diagnosticsStreamMessage.getProcessorStopEvents(), getProcessorStopEventList());
  }

  /**
   * Tests serialization and deserialization of a {@link DiagnosticsStreamMessage}
   */
  @Test
  public void serdeTest() {
    DiagnosticsStreamMessage diagnosticsStreamMessage = getDiagnosticsStreamMessage();
    Collection<DiagnosticsExceptionEvent> exceptionEventList = getExceptionList();
    diagnosticsStreamMessage.addDiagnosticsExceptionEvents(exceptionEventList);
    diagnosticsStreamMessage.addProcessorStopEvents(getProcessorStopEventList());
    diagnosticsStreamMessage.addContainerModels(getSampleContainerModels());

    MetricsSnapshot metricsSnapshot = diagnosticsStreamMessage.convertToMetricsSnapshot();
    Assert.assertEquals(metricsSnapshot.getHeader().getJobName(), jobName);
    Assert.assertEquals(metricsSnapshot.getHeader().getJobId(), jobId);
    Assert.assertEquals(metricsSnapshot.getHeader().getExecEnvironmentContainerId(), executionEnvContainerId);
    Assert.assertEquals(metricsSnapshot.getHeader().getVersion(), taskClassVersion);
    Assert.assertEquals(metricsSnapshot.getHeader().getSamzaVersion(), samzaVersion);
    Assert.assertEquals(metricsSnapshot.getHeader().getHost(), hostname);
    Assert.assertEquals(metricsSnapshot.getHeader().getSource(), DiagnosticsManager.class.getName());

    Map<String, Map<String, Object>> metricsMap = metricsSnapshot.getMetrics().getAsMap();
    Assert.assertTrue(metricsMap.get("org.apache.samza.container.SamzaContainerMetrics").containsKey("exceptions"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("containerModels"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("numPersistentStores"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("containerNumCores"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("containerMemoryMb"));
    Assert.assertTrue(metricsMap.get(DiagnosticsManager.class.getName()).containsKey("stopEvents"));

    DiagnosticsStreamMessage convertedDiagnosticsStreamMessage =
        DiagnosticsStreamMessage.convertToDiagnosticsStreamMessage(metricsSnapshot);

    Assert.assertTrue(convertedDiagnosticsStreamMessage.equals(diagnosticsStreamMessage));
  }
}
