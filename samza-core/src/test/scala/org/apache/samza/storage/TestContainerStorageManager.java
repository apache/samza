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
package org.apache.samza.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstance;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.system.SystemConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class TestContainerStorageManager {

  private ContainerStorageManager containerStorageManager;
  private Map<String, SystemConsumer> systemConsumers;
  private Map<TaskName, TaskStorageManager> taskStorageManagers;
  private SamzaContainerMetrics samzaContainerMetrics;

  private CountDownLatch taskStorageManagersRestoreStoreCount;
  private CountDownLatch taskStorageManagersInitCount;
  private CountDownLatch taskStorageManagersRestoreStopCount;

  private CountDownLatch systemConsumerStartCount;
  private CountDownLatch systemConsumerStopCount;

  private Map<TaskName, Gauge<Object>> taskRestoreMetricGauges;

  /**
   * Utility method for creating a mocked taskInstance and taskStorageManager and adding it to the map.
   * @param taskname the desired taskname.
   */
  private void addMockedTask(String taskname) {
    TaskInstance mockTaskInstance = Mockito.mock(TaskInstance.class);
    Mockito.doAnswer(invocation -> {
        return new TaskName(taskname);
      }).when(mockTaskInstance).taskName();

    TaskStorageManager mockTaskStorageManager = Mockito.mock(TaskStorageManager.class);
    Mockito.doAnswer(invocation -> {
        taskStorageManagersInitCount.countDown();
        return null;
      }).when(mockTaskStorageManager).init();

    Mockito.doAnswer(invocation -> {
        taskStorageManagersRestoreStopCount.countDown();
        return null;
      }).when(mockTaskStorageManager).stop();

//    Mockito.doAnswer(invocation -> {
//        taskStorageManagersRestoreStoreCount.countDown();
//        return null;
//      }).when(mockTaskStorageManager).restoreStores();

    taskStorageManagers.put(new TaskName(taskname), mockTaskStorageManager);

    Gauge testGauge = Mockito.mock(Gauge.class);
    this.taskRestoreMetricGauges.put(new TaskName(taskname), testGauge);
  }

  @Before
  public void setUp() {
    taskRestoreMetricGauges = new HashMap<>();
    systemConsumers = new HashMap<>();
    taskStorageManagers = new HashMap<>();

    // add two mocked tasks
    addMockedTask("task 1");
    addMockedTask("task 2");

    // define the expected number of invocations on taskStorageManagers' init, stop and restore count
    // and the expected number of sysConsumer start and stop
    this.taskStorageManagersInitCount = new CountDownLatch(2);
    this.taskStorageManagersRestoreStoreCount = new CountDownLatch(2);
    this.taskStorageManagersRestoreStopCount = new CountDownLatch(2);
    this.systemConsumerStartCount = new CountDownLatch(1);
    this.systemConsumerStopCount = new CountDownLatch(1);

    // mock container metrics
    samzaContainerMetrics = Mockito.mock(SamzaContainerMetrics.class);
    Mockito.when(samzaContainerMetrics.taskStoreRestorationMetrics()).thenReturn(taskRestoreMetricGauges);

    // mock and setup sysconsumers
    SystemConsumer mockSystemConsumer = Mockito.mock(SystemConsumer.class);
    Mockito.doAnswer(invocation -> {
        systemConsumerStartCount.countDown();
        return null;
      }).when(mockSystemConsumer).start();
    Mockito.doAnswer(invocation -> {
        systemConsumerStopCount.countDown();
        return null;
      }).when(mockSystemConsumer).stop();

    systemConsumers.put("kafka", mockSystemConsumer);

//    this.containerStorageManager =
//        new ContainerStorageManager(taskStorageManagers, systemConsumers, samzaContainerMetrics);
  }

  @Test
  public void testParallelismAndMetrics() {
    this.containerStorageManager.start();
    this.containerStorageManager.shutdown();
    Assert.assertTrue("init count should be 0", this.taskStorageManagersInitCount.getCount() == 0);
    Assert.assertTrue("Restore count should be 0", this.taskStorageManagersRestoreStoreCount.getCount() == 0);
    Assert.assertTrue("stop count should be 0", this.taskStorageManagersRestoreStopCount.getCount() == 0);

    Assert.assertTrue("systemConsumerStopCount count should be 0", this.systemConsumerStopCount.getCount() == 0);
    Assert.assertTrue("systemConsumerStartCount count should be 0", this.systemConsumerStartCount.getCount() == 0);

    for (Gauge gauge : taskRestoreMetricGauges.values()) {
      Assert.assertTrue("Restoration time gauge value should be invoked atleast once", Mockito.mockingDetails(gauge).getInvocations().size() >= 1);
    }
  }

}
