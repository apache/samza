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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstance;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerdeFactory;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.util.SystemClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import scala.collection.JavaConverters;


public class TestContainerStorageManager {

  private static final String STORE_NAME = "store";
  private static final String SYSTEM_NAME = "kafka";
  private static final String STREAM_NAME = "store-stream";

  private ContainerStorageManager containerStorageManager;
  private Map<TaskName, Gauge<Object>> taskRestoreMetricGauges;
  private Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics;
  private SamzaContainerMetrics samzaContainerMetrics;
  private Map<TaskName, TaskModel> tasks;

  private volatile int systemConsumerCreationCount;
  private volatile int systemConsumerStartCount;
  private volatile int systemConsumerStopCount;
  private volatile int storeRestoreCallCount;

  /**
   * Utility method for creating a mocked taskInstance and taskStorageManager and adding it to the map.
   * @param taskname the desired taskname.
   */
  private void addMockedTask(String taskname, int changelogPartition) {
    TaskInstance mockTaskInstance = Mockito.mock(TaskInstance.class);
    Mockito.doAnswer(invocation -> {
        return new TaskName(taskname);
      }).when(mockTaskInstance).taskName();

    Gauge testGauge = Mockito.mock(Gauge.class);
    this.tasks.put(new TaskName(taskname),
        new TaskModel(new TaskName(taskname), new HashSet<>(), new Partition(changelogPartition)));
    this.taskRestoreMetricGauges.put(new TaskName(taskname), testGauge);
    this.taskInstanceMetrics.put(new TaskName(taskname), Mockito.mock(TaskInstanceMetrics.class));
  }

  /**
   * Method to create a containerStorageManager with mocked dependencies
   */
  @Before
  public void setUp() {
    taskRestoreMetricGauges = new HashMap<>();
    this.tasks = new HashMap<>();
    this.taskInstanceMetrics = new HashMap<>();

    // Add two mocked tasks
    addMockedTask("task 0", 0);
    addMockedTask("task 1", 1);

    // Mock container metrics
    samzaContainerMetrics = Mockito.mock(SamzaContainerMetrics.class);
    Mockito.when(samzaContainerMetrics.taskStoreRestorationMetrics()).thenReturn(taskRestoreMetricGauges);

    // Create a map of test changeLogSSPs
    Map<String, SystemStream> changelogSystemStreams = new HashMap<>();
    changelogSystemStreams.put(STORE_NAME, new SystemStream(SYSTEM_NAME, STREAM_NAME));

    // Create mocked storage engine factories
    Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories = new HashMap<>();
    StorageEngineFactory mockStorageEngineFactory =
        (StorageEngineFactory<Object, Object>) Mockito.mock(StorageEngineFactory.class);
    StorageEngine mockStorageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doAnswer(invocation -> {
        return mockStorageEngine;
      }).when(mockStorageEngineFactory).getStorageEngine(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    storageEngineFactories.put(STORE_NAME, mockStorageEngineFactory);

    // Add instrumentation to mocked storage engine, to record the number of store.restore() calls
    Mockito.doAnswer(invocation -> {
        storeRestoreCallCount++;
        return null;
      }).when(mockStorageEngine).restore(Mockito.any());

    // Set the mocked stores' properties to be persistent
    Mockito.doAnswer(invocation -> {
        return new StoreProperties.StorePropertiesBuilder().setLoggedStore(true).build();
      }).when(mockStorageEngine).getStoreProperties();

    // Mock and setup sysconsumers
    SystemConsumer mockSystemConsumer = Mockito.mock(SystemConsumer.class);
    Mockito.doAnswer(invocation -> {
        systemConsumerStartCount++;
        return null;
      }).when(mockSystemConsumer).start();
    Mockito.doAnswer(invocation -> {
        systemConsumerStopCount++;
        return null;
      }).when(mockSystemConsumer).stop();

    // Create mocked system factories
    Map<String, SystemFactory> systemFactories = new HashMap<>();

    // Count the number of sysConsumers created
    SystemFactory mockSystemFactory = Mockito.mock(SystemFactory.class);
    Mockito.doAnswer(invocation -> {
        this.systemConsumerCreationCount++;
        return mockSystemConsumer;
      }).when(mockSystemFactory).getConsumer(Mockito.anyString(), Mockito.any(), Mockito.any());

    systemFactories.put(SYSTEM_NAME, mockSystemFactory);

    // Create mocked configs for specifying serdes
    Map<String, String> configMap = new HashMap<>();
    configMap.put("stores." + STORE_NAME + ".key.serde", "stringserde");
    configMap.put("stores." + STORE_NAME + ".msg.serde", "stringserde");
    configMap.put("serializers.registry.stringserde.class", StringSerdeFactory.class.getName());
    Config config = new MapConfig(configMap);

    Map<String, Serde<Object>> serdes = new HashMap<>();
    serdes.put("stringserde", Mockito.mock(Serde.class));

    // Create mocked system admins
    SystemAdmin mockSystemAdmin = Mockito.mock(SystemAdmin.class);
    Mockito.doAnswer(new Answer<Void>() {
        public Void answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          System.out.println("called with arguments: " + Arrays.toString(args));
          return null;
        }
      }).when(mockSystemAdmin).validateStream(Mockito.any());
    SystemAdmins mockSystemAdmins = Mockito.mock(SystemAdmins.class);
    Mockito.when(mockSystemAdmins.getSystemAdmin("kafka")).thenReturn(mockSystemAdmin);

    // Create a mocked mockStreamMetadataCache
    SystemStreamMetadata.SystemStreamPartitionMetadata sspMetadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("0", "50", "51");
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata = new HashMap<>();
    partitionMetadata.put(new Partition(0), sspMetadata);
    partitionMetadata.put(new Partition(1), sspMetadata);
    SystemStreamMetadata systemStreamMetadata = new SystemStreamMetadata(STREAM_NAME, partitionMetadata);
    StreamMetadataCache mockStreamMetadataCache = Mockito.mock(StreamMetadataCache.class);

    Mockito.when(mockStreamMetadataCache.
        getStreamMetadata(JavaConverters.
            asScalaSetConverter(new HashSet<SystemStream>(changelogSystemStreams.values())).asScala().toSet(), false))
        .thenReturn(
            new scala.collection.immutable.Map.Map1(new SystemStream(SYSTEM_NAME, STREAM_NAME), systemStreamMetadata));

    // Reset the  expected number of sysConsumer create, start and stop calls, and store.restore() calls
    this.systemConsumerCreationCount = 0;
    this.systemConsumerStartCount = 0;
    this.systemConsumerStopCount = 0;
    this.storeRestoreCallCount = 0;

    // Create the container storage manager
    this.containerStorageManager =
        new ContainerStorageManager(new ContainerModel("samza-container-test", tasks), mockStreamMetadataCache,
            mockSystemAdmins, changelogSystemStreams, storageEngineFactories, systemFactories, serdes, config,
            taskInstanceMetrics, samzaContainerMetrics, Mockito.mock(JobContext.class),
            Mockito.mock(ContainerContext.class), Mockito.mock(Map.class), Optional.empty(), Optional.empty(), 2, new SystemClock());
  }

  @Test
  public void testParallelismAndMetrics() {
    this.containerStorageManager.start();
    this.containerStorageManager.shutdown();

    for (Gauge gauge : taskRestoreMetricGauges.values()) {
      Assert.assertTrue("Restoration time gauge value should be invoked atleast once",
          Mockito.mockingDetails(gauge).getInvocations().size() >= 1);
    }

    Assert.assertTrue("Store restore count should be 2 because there are 2 tasks", this.storeRestoreCallCount == 2);
    Assert.assertTrue("systemConsumerCreation count should be 1 (1 consumer per system)",
        this.systemConsumerCreationCount == 1);
    Assert.assertTrue("systemConsumerStopCount count should be 1", this.systemConsumerStopCount == 1);
    Assert.assertTrue("systemConsumerStartCount count should be 1", this.systemConsumerStartCount == 1);
  }
}
