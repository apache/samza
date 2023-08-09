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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointV1;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstance;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerdeFactory;
import org.apache.samza.storage.blobstore.BlobStoreManager;
import org.apache.samza.storage.blobstore.BlobStoreManagerFactory;
import org.apache.samza.storage.blobstore.BlobStoreRestoreManager;
import org.apache.samza.storage.blobstore.BlobStoreStateBackendFactory;
import org.apache.samza.storage.blobstore.Metadata;
import org.apache.samza.storage.blobstore.exceptions.DeletedException;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.apache.samza.storage.blobstore.index.serde.SnapshotIndexSerde;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.SystemClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.collection.JavaConverters;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtil.class, ContainerStorageManagerRestoreUtil.class})
public class TestContainerStorageManager {

  private static final String STORE_NAME = "store";
  private static final String SYSTEM_NAME = "kafka";
  private static final String STREAM_NAME = "store-stream";
  private static final File DEFAULT_STORE_BASE_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "store");
  private static final File
      DEFAULT_LOGGED_STORE_BASE_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "loggedStore");

  private ContainerStorageManager containerStorageManager;
  private Map<TaskName, Gauge<Object>> taskRestoreMetricGauges;
  private Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics;
  private SamzaContainerMetrics samzaContainerMetrics;
  private Map<TaskName, TaskModel> tasks;
  private StandbyTestContext testContext;

  private volatile int systemConsumerCreationCount;
  private volatile int systemConsumerStartCount;
  private volatile int systemConsumerStopCount;
  private volatile int storeRestoreCallCount;

  /**
   * Utility method for creating a mocked taskInstance and taskStorageManager and adding it to the map.
   * @param taskname the desired taskname.
   */
  private void addMockedTask(String taskname, int changelogPartition) {
    TaskInstance mockTaskInstance = mock(TaskInstance.class);
    doAnswer(invocation -> {
      return new TaskName(taskname);
    }).when(mockTaskInstance).taskName();

    Gauge testGauge = mock(Gauge.class);
    this.tasks.put(new TaskName(taskname),
        new TaskModel(new TaskName(taskname), new HashSet<>(), new Partition(changelogPartition)));
    this.taskRestoreMetricGauges.put(new TaskName(taskname), testGauge);
    this.taskInstanceMetrics.put(new TaskName(taskname), mock(TaskInstanceMetrics.class));
  }

  /**
   * Method to create a containerStorageManager with mocked dependencies
   */
  @Before
  public void setUp() throws InterruptedException {
    taskRestoreMetricGauges = new HashMap<>();
    this.tasks = new HashMap<>();
    this.taskInstanceMetrics = new HashMap<>();

    // Add two mocked tasks
    addMockedTask("task 0", 0);
    addMockedTask("task 1", 1);

    // Mock container metrics
    samzaContainerMetrics = mock(SamzaContainerMetrics.class);
    when(samzaContainerMetrics.taskStoreRestorationMetrics()).thenReturn(taskRestoreMetricGauges);

    // Create a map of test changeLogSSPs
    Map<String, SystemStream> changelogSystemStreams = new HashMap<>();
    changelogSystemStreams.put(STORE_NAME, new SystemStream(SYSTEM_NAME, STREAM_NAME));

    // Create mocked storage engine factories
    Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories = new HashMap<>();
    StorageEngineFactory mockStorageEngineFactory =
        (StorageEngineFactory<Object, Object>) mock(StorageEngineFactory.class);
    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    when(mockStorageEngine.getStoreProperties())
        .thenReturn(new StoreProperties.StorePropertiesBuilder().setLoggedStore(true).setPersistedToDisk(true).build());
    doAnswer(invocation -> {
      return mockStorageEngine;
    }).when(mockStorageEngineFactory).getStorageEngine(anyString(), any(), any(), any(), any(),
        any(), any(), any(), any(), any());

    storageEngineFactories.put(STORE_NAME, mockStorageEngineFactory);

    // Add instrumentation to mocked storage engine, to record the number of store.restore() calls
    doAnswer(invocation -> {
      storeRestoreCallCount++;
      return CompletableFuture.completedFuture(null);
    }).when(mockStorageEngine).restore(any());

    // Set the mocked stores' properties to be persistent
    doAnswer(invocation -> {
      return new StoreProperties.StorePropertiesBuilder().setLoggedStore(true).build();
    }).when(mockStorageEngine).getStoreProperties();

    // Mock and setup sysconsumers
    SystemConsumer mockSystemConsumer = mock(SystemConsumer.class);
    doAnswer(invocation -> {
      systemConsumerStartCount++;
      return null;
    }).when(mockSystemConsumer).start();
    doAnswer(invocation -> {
      systemConsumerStopCount++;
      return null;
    }).when(mockSystemConsumer).stop();

    // Create mocked system factories
    Map<String, SystemFactory> systemFactories = new HashMap<>();

    // Count the number of sysConsumers created
    SystemFactory mockSystemFactory = mock(SystemFactory.class);
    doAnswer(invocation -> {
      this.systemConsumerCreationCount++;
      return mockSystemConsumer;
    }).when(mockSystemFactory).getConsumer(anyString(), any(), any());

    systemFactories.put(SYSTEM_NAME, mockSystemFactory);

    // Create mocked configs for specifying serdes
    Map<String, String> configMap = new HashMap<>();
    configMap.put("stores." + STORE_NAME + ".key.serde", "stringserde");
    configMap.put("stores." + STORE_NAME + ".msg.serde", "stringserde");
    configMap.put("stores." + STORE_NAME + ".factory", mockStorageEngineFactory.getClass().getName());
    configMap.put("stores." + STORE_NAME + ".changelog", SYSTEM_NAME + "." + STREAM_NAME);
    configMap.put("serializers.registry.stringserde.class", StringSerdeFactory.class.getName());
    configMap.put(TaskConfig.TRANSACTIONAL_STATE_RETAIN_EXISTING_STATE, "true");
    Config config = new MapConfig(configMap);

    Map<String, Serde<Object>> serdes = new HashMap<>();
    serdes.put("stringserde", mock(Serde.class));

    // Create mocked system admins
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    doAnswer(new Answer<Void>() {
        public Void answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          System.out.println("called with arguments: " + Arrays.toString(args));
          return null;
        }
      }).when(mockSystemAdmin).validateStream(any());
    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    when(mockSystemAdmins.getSystemAdmin("kafka")).thenReturn(mockSystemAdmin);

    // Create a mocked mockStreamMetadataCache
    SystemStreamMetadata.SystemStreamPartitionMetadata sspMetadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("0", "50", "51");
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata = new HashMap<>();
    partitionMetadata.put(new Partition(0), sspMetadata);
    partitionMetadata.put(new Partition(1), sspMetadata);
    SystemStreamMetadata systemStreamMetadata = new SystemStreamMetadata(STREAM_NAME, partitionMetadata);
    StreamMetadataCache mockStreamMetadataCache = mock(StreamMetadataCache.class);

    when(mockStreamMetadataCache.
        getStreamMetadata(JavaConverters.
            asScalaSetConverter(new HashSet<SystemStream>(changelogSystemStreams.values())).asScala().toSet(), false))
        .thenReturn(
            new scala.collection.immutable.Map.Map1(new SystemStream(SYSTEM_NAME, STREAM_NAME), systemStreamMetadata));

    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    when(checkpointManager.readLastCheckpoint(any(TaskName.class))).thenReturn(new CheckpointV1(new HashMap<>()));

    SSPMetadataCache mockSSPMetadataCache = mock(SSPMetadataCache.class);
    when(mockSSPMetadataCache.getMetadata(any(SystemStreamPartition.class)))
        .thenReturn(new SystemStreamMetadata.SystemStreamPartitionMetadata("0", "10", "11"));

    ContainerContext mockContainerContext = mock(ContainerContext.class);
    ContainerModel mockContainerModel = new ContainerModel("samza-container-test", tasks);
    when(mockContainerContext.getContainerModel()).thenReturn(mockContainerModel);

    // Reset the expected number of sysConsumer create, start and stop calls, and store.restore() calls
    this.systemConsumerCreationCount = 0;
    this.systemConsumerStartCount = 0;
    this.systemConsumerStopCount = 0;
    this.storeRestoreCallCount = 0;

    StateBackendFactory backendFactory = mock(StateBackendFactory.class);
    TaskRestoreManager restoreManager = mock(TaskRestoreManager.class);
    ArgumentCaptor<ExecutorService> restoreExecutorCaptor = ArgumentCaptor.forClass(ExecutorService.class);
    when(backendFactory.getRestoreManager(any(), any(), any(), restoreExecutorCaptor.capture(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(restoreManager);
    doAnswer(invocation -> {
      storeRestoreCallCount++;
      return CompletableFuture.completedFuture(null);
    }).when(restoreManager).restore();

    Map<TaskName, TaskInstanceCollector> taskInstanceCollectors = new HashMap<>();
    tasks.keySet().forEach(taskName -> taskInstanceCollectors.put(taskName, mock(TaskInstanceCollector.class)));

    // Create the container storage manager
    this.containerStorageManager = new ContainerStorageManager(
        checkpointManager,
        mockContainerModel,
        mockStreamMetadataCache,
        mockSystemAdmins,
        changelogSystemStreams,
        new HashMap<>(),
        storageEngineFactories,
        systemFactories,
        serdes,
        config,
        taskInstanceMetrics,
        samzaContainerMetrics,
        mock(JobContext.class),
        mockContainerContext,
        ImmutableMap.of(StorageConfig.KAFKA_STATE_BACKEND_FACTORY, backendFactory),
        taskInstanceCollectors,
        DEFAULT_LOGGED_STORE_BASE_DIR,
        DEFAULT_STORE_BASE_DIR,
        null,
        SystemClock.instance());
    this.testContext = new StandbyTestContext();
  }

  @Test
  public void testParallelismAndMetrics() throws InterruptedException {
    this.containerStorageManager.start();
    this.containerStorageManager.shutdown();

    for (Gauge gauge : taskRestoreMetricGauges.values()) {
      Assert.assertTrue("Restoration time gauge value should be invoked atleast once",
          mockingDetails(gauge).getInvocations().size() >= 1);
    }

    Assert.assertEquals("Store restore count should be 2 because there are 2 tasks", 2, this.storeRestoreCallCount);
    Assert.assertEquals("systemConsumerCreation count should be 1 (1 consumer per system)", 1,
        this.systemConsumerCreationCount);
    Assert.assertEquals("systemConsumerStopCount count should be 1", 1, this.systemConsumerStopCount);
    Assert.assertEquals("systemConsumerStartCount count should be 1", 1, this.systemConsumerStartCount);
  }

  @Test
  public void testNoConfiguredDurableStores() throws InterruptedException {
    taskRestoreMetricGauges = new HashMap<>();
    this.tasks = new HashMap<>();
    this.taskInstanceMetrics = new HashMap<>();

    // Add two mocked tasks
    addMockedTask("task 0", 0);
    addMockedTask("task 1", 1);

    // Mock container metrics
    samzaContainerMetrics = mock(SamzaContainerMetrics.class);
    when(samzaContainerMetrics.taskStoreRestorationMetrics()).thenReturn(taskRestoreMetricGauges);

    // Create mocked configs for specifying serdes
    Map<String, String> configMap = new HashMap<>();
    configMap.put("serializers.registry.stringserde.class", StringSerdeFactory.class.getName());
    configMap.put(TaskConfig.TRANSACTIONAL_STATE_RETAIN_EXISTING_STATE, "true");
    Config config = new MapConfig(configMap);

    Map<String, Serde<Object>> serdes = new HashMap<>();
    serdes.put("stringserde", mock(Serde.class));

    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    when(checkpointManager.readLastCheckpoint(any(TaskName.class))).thenReturn(new CheckpointV1(new HashMap<>()));

    ContainerContext mockContainerContext = mock(ContainerContext.class);
    ContainerModel mockContainerModel = new ContainerModel("samza-container-test", tasks);
    when(mockContainerContext.getContainerModel()).thenReturn(mockContainerModel);

    // Reset the expected number of sysConsumer create, start and stop calls, and store.restore() calls
    this.systemConsumerCreationCount = 0;
    this.systemConsumerStartCount = 0;
    this.systemConsumerStopCount = 0;
    this.storeRestoreCallCount = 0;

    StateBackendFactory backendFactory = mock(StateBackendFactory.class);
    TaskRestoreManager restoreManager = mock(TaskRestoreManager.class);
    when(backendFactory.getRestoreManager(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(restoreManager);
    doAnswer(invocation -> {
      storeRestoreCallCount++;
      return CompletableFuture.completedFuture(null);
    }).when(restoreManager).restore();

    // Create the container storage manager
    ContainerStorageManager containerStorageManager = new ContainerStorageManager(
        checkpointManager,
        mockContainerModel,
        mock(StreamMetadataCache.class),
        mock(SystemAdmins.class),
        new HashMap<>(),
        new HashMap<>(),
        new HashMap<>(),
        new HashMap<>(),
        serdes,
        config,
        taskInstanceMetrics,
        samzaContainerMetrics,
        mock(JobContext.class),
        mockContainerContext,
        new HashMap<>(),
        mock(Map.class),
        DEFAULT_LOGGED_STORE_BASE_DIR,
        DEFAULT_STORE_BASE_DIR,
        null,
        SystemClock.instance());

    containerStorageManager.start();
    containerStorageManager.shutdown();

    for (Gauge gauge : taskRestoreMetricGauges.values()) {
      Assert.assertTrue("Restoration time gauge value should never be invoked",
          mockingDetails(gauge).getInvocations().size() == 0);
    }

    Assert.assertEquals("Store restore count should be 2 because there are 0 stores", 0, this.storeRestoreCallCount);
    Assert.assertEquals(0,
        this.systemConsumerCreationCount);
    Assert.assertEquals(0, this.systemConsumerStopCount);
    Assert.assertEquals(0, this.systemConsumerStartCount);
  }

  @Test
  public void testCheckpointBasedRestoreFactoryCreation() {
    Set<String> storeNames = ImmutableSet.of("storeName0", "storeName1", "storeName2");

    StorageConfig mockConfig = mock(StorageConfig.class);
    when(mockConfig.getStoreRestoreFactories("storeName0"))
        .thenReturn(ImmutableList.of("factory0", "factory1", "factory2"));
    when(mockConfig.getStoreRestoreFactories("storeName1"))
        .thenReturn(ImmutableList.of("factory2", "factory1"));
    when(mockConfig.getStoreRestoreFactories("storeName2"))
        .thenReturn(Collections.emptyList());

    when(mockConfig.getChangelogStream("storeName0"))
        .thenReturn(Optional.empty());
    when(mockConfig.getChangelogStream("storeName1"))
        .thenReturn(Optional.of("changelog"));
    when(mockConfig.getChangelogStream("storeName2"))
        .thenReturn(Optional.of("changelog"));

    CheckpointV1 checkpointV1 = mock(CheckpointV1.class);
    when(checkpointV1.getVersion()).thenReturn((short) 1);
    Map<String, Set<String>> factoriesToStores =
        ContainerStorageManagerUtil.getBackendFactoryStoreNames(storeNames, checkpointV1, mockConfig);

    Assert.assertEquals(1, factoriesToStores.size());
    Assert.assertEquals(ImmutableSet.of("storeName1", "storeName2"),
        factoriesToStores.get(StorageConfig.KAFKA_STATE_BACKEND_FACTORY));

    factoriesToStores = ContainerStorageManagerUtil.getBackendFactoryStoreNames(storeNames, null, mockConfig);

    Assert.assertEquals(2, factoriesToStores.size());
    Assert.assertEquals(ImmutableSet.of("storeName0"),
        factoriesToStores.get("factory0"));
    Assert.assertEquals(ImmutableSet.of("storeName1"),
        factoriesToStores.get("factory2"));
  }

  @Test
  public void testCheckpointV2BasedRestoreFactoryCreation() {
    Set<String> storeNames = ImmutableSet.of("storeName0", "storeName1", "storeName2");

    StorageConfig mockConfig = mock(StorageConfig.class);
    when(mockConfig.getStoreRestoreFactories("storeName0"))
        .thenReturn(ImmutableList.of("factory0", "factory1", "factory2"));
    when(mockConfig.getStoreRestoreFactories("storeName1"))
        .thenReturn(ImmutableList.of("factory2", "factory1"));
    when(mockConfig.getStoreRestoreFactories("storeName2"))
        .thenReturn(Collections.emptyList());

    when(mockConfig.getChangelogStream("storeName0"))
        .thenReturn(Optional.empty());
    when(mockConfig.getChangelogStream("storeName1"))
        .thenReturn(Optional.of("changelog"));
    when(mockConfig.getChangelogStream("storeName2"))
        .thenReturn(Optional.of("changelog"));

    CheckpointV2 checkpointV2 = mock(CheckpointV2.class);
    when(checkpointV2.getVersion()).thenReturn((short) 2);
    when(checkpointV2.getStateCheckpointMarkers())
        .thenReturn(ImmutableMap.of(
            "factory0", ImmutableMap.of("storeName0", "", "storeName1", "", "storeName2", ""),
            "factory1", ImmutableMap.of("storeName0", "", "storeName1", "", "storeName2", ""),
            "factory2", ImmutableMap.of("storeName0", "", "storeName1", "", "storeName2", "")));

    Map<String, Set<String>> factoriesToStores =
        ContainerStorageManagerUtil.getBackendFactoryStoreNames(storeNames, checkpointV2, mockConfig);
    Assert.assertEquals(2, factoriesToStores.size());
    Assert.assertEquals(ImmutableSet.of("storeName0"),
        factoriesToStores.get("factory0"));
    Assert.assertEquals(ImmutableSet.of("storeName1"),
        factoriesToStores.get("factory2"));

    when(checkpointV2.getStateCheckpointMarkers())
        .thenReturn(ImmutableMap.of(
            "factory2", ImmutableMap.of("storeName0", "", "storeName1", "", "storeName2", "")));
    factoriesToStores = ContainerStorageManagerUtil.getBackendFactoryStoreNames(storeNames, checkpointV2, mockConfig);
    Assert.assertEquals(1, factoriesToStores.size());
    Assert.assertEquals(ImmutableSet.of("storeName1", "storeName0"),
        factoriesToStores.get("factory2"));

    when(checkpointV2.getStateCheckpointMarkers())
        .thenReturn(ImmutableMap.of(
            "factory1", ImmutableMap.of("storeName0", "", "storeName1", "", "storeName2", ""),
            "factory2", ImmutableMap.of("storeName0", "", "storeName1", "", "storeName2", "")));
    factoriesToStores = ContainerStorageManagerUtil.getBackendFactoryStoreNames(storeNames, checkpointV2, mockConfig);
    Assert.assertEquals(2, factoriesToStores.size());
    Assert.assertEquals(ImmutableSet.of("storeName0"),
        factoriesToStores.get("factory1"));
    Assert.assertEquals(ImmutableSet.of("storeName1"),
        factoriesToStores.get("factory2"));

    when(checkpointV2.getStateCheckpointMarkers())
        .thenReturn(ImmutableMap.of(
            "factory1", ImmutableMap.of("storeName0", "", "storeName1", "", "storeName2", ""),
            "factory2", ImmutableMap.of("storeName0", "", "storeName2", "")));
    factoriesToStores = ContainerStorageManagerUtil.getBackendFactoryStoreNames(storeNames, checkpointV2, mockConfig);
    Assert.assertEquals(1, factoriesToStores.size());
    Assert.assertEquals(ImmutableSet.of("storeName0", "storeName1"),
        factoriesToStores.get("factory1"));

    when(checkpointV2.getStateCheckpointMarkers())
        .thenReturn(ImmutableMap.of(
            "factory1", ImmutableMap.of("storeName0", "", "storeName1", "", "storeName2", "")));
    factoriesToStores = ContainerStorageManagerUtil.getBackendFactoryStoreNames(storeNames, checkpointV2, mockConfig);
    Assert.assertEquals(1, factoriesToStores.size());
    Assert.assertEquals(ImmutableSet.of("storeName0", "storeName1"),
        factoriesToStores.get("factory1"));

    when(checkpointV2.getStateCheckpointMarkers())
        .thenReturn(Collections.emptyMap());
    factoriesToStores = ContainerStorageManagerUtil.getBackendFactoryStoreNames(storeNames, checkpointV2, mockConfig);
    Assert.assertEquals(2, factoriesToStores.size());
    Assert.assertEquals(ImmutableSet.of("storeName0"),
        factoriesToStores.get("factory0"));
    Assert.assertEquals(ImmutableSet.of("storeName1"),
        factoriesToStores.get("factory2"));

    when(checkpointV2.getStateCheckpointMarkers())
        .thenReturn(ImmutableMap.of(
            "factory0", ImmutableMap.of("storeName1", "", "storeName2", ""),
            "factory1", ImmutableMap.of("storeName1", "", "storeName2", ""),
            "factory2", ImmutableMap.of("storeName0", "", "storeName2", "")));
    factoriesToStores = ContainerStorageManagerUtil.getBackendFactoryStoreNames(storeNames, checkpointV2, mockConfig);
    Assert.assertEquals(2, factoriesToStores.size());
    Assert.assertEquals(ImmutableSet.of("storeName1"),
        factoriesToStores.get("factory1"));
    Assert.assertEquals(ImmutableSet.of("storeName0"),
        factoriesToStores.get("factory2"));
  }

  @Test
  public void testInitRecoversFromDeletedException() {
    TaskName taskName = new TaskName("task");
    Set<String> stores = Collections.singleton("store");

    BlobStoreRestoreManager taskRestoreManager = mock(BlobStoreRestoreManager.class);
    Throwable deletedException = new SamzaException(new CompletionException(new DeletedException("410 gone")));
    doThrow(deletedException).when(taskRestoreManager).init(any(Checkpoint.class));
    when(taskRestoreManager.restore()).thenReturn(CompletableFuture.completedFuture(null));
    when(taskRestoreManager.restore(true)).thenReturn(CompletableFuture.completedFuture(null));

    // mock ReflectionUtil.getObj
    PowerMockito.mockStatic(ReflectionUtil.class);
    BlobStoreManagerFactory blobStoreManagerFactory = mock(BlobStoreManagerFactory.class);
    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);
    PowerMockito.when(ReflectionUtil.getObj(anyString(), eq(BlobStoreManagerFactory.class)))
        .thenReturn(blobStoreManagerFactory);
    when(blobStoreManagerFactory.getRestoreBlobStoreManager(any(Config.class), any(ExecutorService.class)))
        .thenReturn(blobStoreManager);

    Map<String, TaskRestoreManager> storeTaskRestoreManager = ImmutableMap.of("store", taskRestoreManager);
    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    JobContext jobContext = mock(JobContext.class);
    when(jobContext.getJobModel()).thenReturn(mock(JobModel.class));
    TaskModel taskModel = mock(TaskModel.class);
    when(taskModel.getTaskName()).thenReturn(new TaskName("test"));
    ContainerModel containerModel = mock(ContainerModel.class);
    when(containerModel.getTasks()).thenReturn(ImmutableMap.of(taskName, taskModel));
    Checkpoint checkpoint = mock(CheckpointV2.class);
    Map<TaskName, Checkpoint> taskCheckpoints = ImmutableMap.of(taskName, checkpoint);
    Map<TaskName, Map<String, Set<String>>> taskBackendFactoryToStoreNames =
        ImmutableMap.of(taskName, ImmutableMap.of(BlobStoreStateBackendFactory.class.getName(), stores));
    Config config = new MapConfig(ImmutableMap.of("job.name", "test"), ImmutableMap.of("stores.store.backup.factories", BlobStoreStateBackendFactory.class.getName()));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    SystemConsumer systemConsumer = mock(SystemConsumer.class);

    ContainerStorageManagerRestoreUtil.initAndRestoreTaskInstances(ImmutableMap.of(taskName, storeTaskRestoreManager),
        samzaContainerMetrics, checkpointManager, jobContext, containerModel, taskCheckpoints,
        taskBackendFactoryToStoreNames, config, executor, new HashMap<>(), null,
        ImmutableMap.of("store", systemConsumer));

    // verify init() is called twice -> once without getDeleted flag, once with getDeleted flag
    verify(taskRestoreManager, times(1)).init(any(Checkpoint.class));
    verify(taskRestoreManager, times(1)).init(any(Checkpoint.class), anyBoolean());
    // verify restore is called with getDeletedFlag only
    verify(taskRestoreManager, times(0)).restore();
    verify(taskRestoreManager, times(1)).restore(true);
  }

  @Test
  public void testRestoreRecoversFromDeletedException() throws Exception {
    TaskName taskName = new TaskName("task");
    Set<String> stores = Collections.singleton("store");

    BlobStoreRestoreManager taskRestoreManager = mock(BlobStoreRestoreManager.class);
    doNothing().when(taskRestoreManager).init(any(Checkpoint.class));

    CompletableFuture<Void> failedFuture = CompletableFuture.completedFuture(null)
        .thenCompose(v -> { throw new DeletedException("410 Gone"); });
    when(taskRestoreManager.restore()).thenReturn(failedFuture);
    when(taskRestoreManager.restore(true)).thenReturn(CompletableFuture.completedFuture(null));

    Map<String, TaskRestoreManager> factoryToTaskRestoreManager = ImmutableMap.of(
        BlobStoreStateBackendFactory.class.getName(), taskRestoreManager);

    JobContext jobContext = mock(JobContext.class);
    TaskModel taskModel = mock(TaskModel.class);
    when(taskModel.getTaskName()).thenReturn(taskName);
    when(taskModel.getTaskMode()).thenReturn(TaskMode.Active);

    ContainerModel containerModel = mock(ContainerModel.class);
    when(containerModel.getTasks()).thenReturn(ImmutableMap.of(taskName, taskModel));

    CheckpointV2 checkpoint = mock(CheckpointV2.class);
    when(checkpoint.getOffsets()).thenReturn(ImmutableMap.of());
    when(checkpoint.getCheckpointId()).thenReturn(CheckpointId.create());
    when(checkpoint.getStateCheckpointMarkers()).thenReturn(ImmutableMap.of(
        KafkaChangelogStateBackendFactory.class.getName(), new HashMap<>()));

    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    when(checkpointManager.readLastCheckpoint(taskName)).thenReturn(checkpoint);

    String expectedOldBlobId = "oldBlobId";
    when(checkpoint.getStateCheckpointMarkers()).thenReturn(ImmutableMap.of(
        BlobStoreStateBackendFactory.class.getName(), ImmutableMap.of("store", expectedOldBlobId)));

    Map<TaskName, Checkpoint> taskCheckpoints = ImmutableMap.of(taskName, checkpoint);

    Map<TaskName, Map<String, Set<String>>> taskBackendFactoryToStoreNames =
        ImmutableMap.of(taskName, ImmutableMap.of(
            BlobStoreStateBackendFactory.class.getName(), stores));

    Config config = new MapConfig(ImmutableMap.of(
        "blob.store.manager.factory", BlobStoreStateBackendFactory.class.getName(),
        "job.name", "test"));

    ExecutorService executor = Executors.newFixedThreadPool(5);

    SystemConsumer systemConsumer = mock(SystemConsumer.class);

    // mock ReflectionUtil.getObj
    PowerMockito.mockStatic(ReflectionUtil.class);
    BlobStoreManagerFactory blobStoreManagerFactory = mock(BlobStoreManagerFactory.class);
    BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);
    PowerMockito.when(ReflectionUtil.getObj(anyString(), eq(BlobStoreManagerFactory.class)))
        .thenReturn(blobStoreManagerFactory);
    when(blobStoreManagerFactory.getRestoreBlobStoreManager(any(Config.class), any(ExecutorService.class)))
        .thenReturn(blobStoreManager);

    // mock ContainerStorageManagerRestoreUtil.backupRecoveredStore
    String expectedBlobId = "blobId";
    PowerMockito.spy(ContainerStorageManagerRestoreUtil.class);
    PowerMockito.doReturn(CompletableFuture.completedFuture(ImmutableMap.of("store", expectedBlobId)))
        .when(ContainerStorageManagerRestoreUtil.class, "backupRecoveredStore",
            any(JobContext.class), any(ContainerModel.class), any(Config.class),
            any(TaskName.class), any(Set.class), any(Checkpoint.class), any(File.class),
            any(BlobStoreManager.class), any(MetricsRegistry.class), any(ExecutorService.class));

    SnapshotIndex snapshotIndex = new SnapshotIndex(System.currentTimeMillis(),
        new SnapshotMetadata(CheckpointId.create(), "job", "test", "task", "store"),
        new DirIndex("test", new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>()),
        Optional.empty());

    ArgumentCaptor<String> getBlobIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<ByteArrayOutputStream> outputStreamCaptor = ArgumentCaptor.forClass(ByteArrayOutputStream.class);
    when(blobStoreManager.get(getBlobIdCaptor.capture(), outputStreamCaptor.capture(),
        any(Metadata.class), any(Boolean.class)))
        .thenAnswer(invocation -> {
          ByteArrayOutputStream outputStream = outputStreamCaptor.getValue();
          outputStream.write(new SnapshotIndexSerde().toBytes(snapshotIndex));
          return CompletableFuture.completedFuture(null);
        });

    ArgumentCaptor<String> removeTTLBlobIdCaptor = ArgumentCaptor.forClass(String.class);
    when(blobStoreManager.removeTTL(removeTTLBlobIdCaptor.capture(), any(Metadata.class)))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(null));

    ArgumentCaptor<String> deleteBlobIdCaptor = ArgumentCaptor.forClass(String.class);
    when(blobStoreManager.delete(deleteBlobIdCaptor.capture(), any(Metadata.class)))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(null));

    Map<TaskName, Checkpoint> updatedTaskCheckpoints =
        ContainerStorageManagerRestoreUtil.initAndRestoreTaskInstances(ImmutableMap.of(taskName, factoryToTaskRestoreManager),
            samzaContainerMetrics, checkpointManager, jobContext, containerModel, taskCheckpoints,
            taskBackendFactoryToStoreNames, config, executor, new HashMap<>(), null,
            ImmutableMap.of("store", systemConsumer)).get();

    // verify taskCheckpoint is updated
    assertNotEquals(((CheckpointV2) taskCheckpoints.get(taskName)).getCheckpointId(),
        ((CheckpointV2) updatedTaskCheckpoints.get(taskName)).getCheckpointId());

    // verify init is not retried with getDeleted
    verify(taskRestoreManager, times(0)).init(any(Checkpoint.class), anyBoolean());

    // verify restore is call twice - once without getDeleted flag, once with getDeleted flag
    verify(taskRestoreManager, times(1)).restore();
    verify(taskRestoreManager, times(1)).restore(true);

    // verify the GET and removeTTL was called on the new SnapshotIndex
    assertEquals(expectedBlobId, getBlobIdCaptor.getAllValues().get(0));
    assertEquals(expectedBlobId, removeTTLBlobIdCaptor.getAllValues().get(0));

    // verify that GET and delete was called on the old SnapshotIndex
    assertEquals(expectedOldBlobId, getBlobIdCaptor.getAllValues().get(1));
    assertEquals(expectedOldBlobId, deleteBlobIdCaptor.getValue());
  }

  @Test
  public void getActiveTaskChangelogSystemStreams() {
    Map<String, SystemStream> storeToChangelogSystemStreams =
        ContainerStorageManagerUtil.getActiveTaskChangelogSystemStreams(testContext.storesToSystemStreams, testContext.standbyContainerModel
        );

    assertEquals("Standby container should have no active change log", Collections.emptyMap(),
        storeToChangelogSystemStreams);
  }

  @Test
  public void getActiveTaskChangelogSystemStreamsForActiveAndStandbyContainer() {
    Map<String, SystemStream> expectedStoreToChangelogSystemStreams =
        testContext.storesToSystemStreams;
    Map<String, SystemStream> storeToChangelogSystemStreams = ContainerStorageManagerUtil.getActiveTaskChangelogSystemStreams(
        testContext.storesToSystemStreams, testContext.activeAndStandbyContainerModel);

    assertEquals("Active and standby container model should have non empty store to changelog mapping",
        expectedStoreToChangelogSystemStreams, storeToChangelogSystemStreams);
  }

  @Test
  public void getActiveTaskChangelogSystemStreamsForStandbyContainer() {
    Map<String, SystemStream> expectedStoreToChangelogSystemStreams =
        testContext.storesToSystemStreams;
    Map<String, SystemStream> storeToChangelogSystemStreams = ContainerStorageManagerUtil.getActiveTaskChangelogSystemStreams(
        testContext.storesToSystemStreams, testContext.activeContainerModel);

    assertEquals("Active container model should have non empty store to changelog mapping",
        expectedStoreToChangelogSystemStreams, storeToChangelogSystemStreams);
  }

  @Test
  public void getSideInputStoresForActiveContainer() {
    Set<String> expectedSideInputStores = testContext.activeStores;
    Set<String> actualSideInputStores =
        ContainerStorageManagerUtil.getSideInputStoreNames(testContext.sideInputStoresToSystemStreams, testContext.storesToSystemStreams, testContext.activeContainerModel
        );

    assertEquals("Mismatch in stores", expectedSideInputStores, actualSideInputStores);
  }

  @Test
  public void getSideInputStoresForStandbyContainer() {
    final Set<String> expectedSideInputStores = testContext.standbyStores;
    Set<String> actualSideInputStores =
        ContainerStorageManagerUtil.getSideInputStoreNames(testContext.sideInputStoresToSystemStreams, testContext.storesToSystemStreams, testContext.standbyContainerModel
        );

    assertEquals("Mismatch in side input stores", expectedSideInputStores, actualSideInputStores);
  }

  @Test
  public void getTaskSideInputSSPsForActiveContainer() {
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> expectedSideInputSSPs = testContext.activeSideInputSSPs;
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> actualSideInputSSPs =
        SideInputsManager.getTaskSideInputSSPs(Collections.emptyMap(), testContext.storesToSystemStreams, testContext.activeContainerModel
        );

    assertEquals("Mismatch in task name --> store --> SSP mapping", expectedSideInputSSPs, actualSideInputSSPs);
  }

  @Test
  public void getTaskSideInputSSPsForStandbyContainerWithSideInput() {
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> expectedSideInputSSPs = testContext.standbyWithSideInputSSPs;
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> actualSideInputSSPs =
        SideInputsManager.getTaskSideInputSSPs(testContext.sideInputStoresToSystemStreams, testContext.storesToSystemStreams, testContext.standbyContainerModelWithSideInputs
        );

    assertEquals("Mismatch in task name --> store --> SSP mapping", expectedSideInputSSPs, actualSideInputSSPs);
  }

  @Test
  public void getTaskSideInputSSPsForStandbyContainerWithoutSideInputs() {
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> expectedSideInputSSPs = testContext.standbyChangelogSSPs;
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> actualSideInputSSPs =
        SideInputsManager.getTaskSideInputSSPs(Collections.emptyMap(), testContext.storesToSystemStreams, testContext.standbyContainerModel
        );

    assertEquals("Mismatch in task name --> store --> SSP mapping", expectedSideInputSSPs, actualSideInputSSPs);
  }

  /**
   * A container class to hold test fields and expected state for standby and side input related tests
   */
  private static class StandbyTestContext {
    private static final String ACTIVE_TASK = "active-task-1";
    private static final TaskName ACTIVE_TASK_NAME = new TaskName(ACTIVE_TASK);
    private static final TaskModel ACTIVE_TASK_MODEL =
        new TaskModel(ACTIVE_TASK_NAME, Collections.emptySet(), new Partition(1));

    private static final String STANDBY_TASK_2 = "standby-task-2";
    private static final TaskName STANDBY_TASK_NAME_2 = new TaskName(STANDBY_TASK_2);
    private static final Set<SystemStreamPartition> STANDBY_TASK_INPUT_SSP =
        ImmutableSet.of(new SystemStreamPartition("test", "side-input-stream", new Partition(2)));
    private static final TaskModel STANDBY_TASK_MODEL_WITH_SIDE_INPUT =
        new TaskModel(STANDBY_TASK_NAME_2, STANDBY_TASK_INPUT_SSP, new Partition(0), TaskMode.Standby);

    private static final String STANDBY_TASK = "standby-task";
    private static final TaskName STANDBY_TASK_NAME = new TaskName(STANDBY_TASK);
    private static final SystemStreamPartition STANDBY_CHANGELOG_SSP =
        new SystemStreamPartition("test", "stream", new Partition(0));
    private static final TaskModel STANDBY_TASK_MODEL =
        new TaskModel(STANDBY_TASK_NAME, Collections.emptySet(), new Partition(0), TaskMode.Standby);

    private static final String SIDE_INPUT_STORE = "side-input-store";
    private static final SystemStream SIDE_INPUT_SYSTEM_STREAM = new SystemStream("test", "side-input-stream");
    private static final String TEST_STORE = "test-store";
    private static final SystemStream TEST_SYSTEM_STREAM = new SystemStream("test", "stream");

    private final ContainerModel activeContainerModel;
    private final ContainerModel activeAndStandbyContainerModel;
    private final ContainerModel standbyContainerModel;
    private final ContainerModel standbyContainerModelWithSideInputs;

    private final Map<TaskName, Map<String, Set<SystemStreamPartition>>> activeSideInputSSPs;
    private final Map<TaskName, Map<String, Set<SystemStreamPartition>>> standbyChangelogSSPs;
    private final Map<TaskName, Map<String, Set<SystemStreamPartition>>> standbyWithSideInputSSPs;

    private final Map<String, SystemStream> storesToSystemStreams;
    private final Map<String, Set<SystemStream>> sideInputStoresToSystemStreams;
    private final Set<String> activeStores;
    private final Set<String> standbyStores;

    public StandbyTestContext() {
      Map<TaskName, TaskModel> activeTasks = ImmutableMap.of(ACTIVE_TASK_NAME, ACTIVE_TASK_MODEL);
      Map<TaskName, TaskModel> standbyTasks = ImmutableMap.of(STANDBY_TASK_NAME, STANDBY_TASK_MODEL);

      activeContainerModel = new ContainerModel("active-container-model", activeTasks);
      activeAndStandbyContainerModel = new ContainerModel("active-standby-container-model",
          ImmutableMap.<TaskName, TaskModel>builder().putAll(activeTasks).putAll(standbyTasks).build());
      standbyContainerModel = new ContainerModel("standby-container-model", standbyTasks);
      standbyContainerModelWithSideInputs = new ContainerModel("standby-container-with-side-input",
          ImmutableMap.of(STANDBY_TASK_NAME_2, STANDBY_TASK_MODEL_WITH_SIDE_INPUT));

      activeStores = ImmutableSet.of(SIDE_INPUT_STORE);
      standbyStores = ImmutableSet.of(SIDE_INPUT_STORE, TEST_STORE);

      sideInputStoresToSystemStreams = ImmutableMap.of(SIDE_INPUT_STORE, ImmutableSet.of(SIDE_INPUT_SYSTEM_STREAM));
      storesToSystemStreams = ImmutableMap.of(TEST_STORE, TEST_SYSTEM_STREAM);

      activeSideInputSSPs = ImmutableMap.of(ACTIVE_TASK_NAME, Collections.emptyMap());
      standbyChangelogSSPs =
          ImmutableMap.of(STANDBY_TASK_NAME, ImmutableMap.of(TEST_STORE, ImmutableSet.of(STANDBY_CHANGELOG_SSP)));
      standbyWithSideInputSSPs = ImmutableMap.of(STANDBY_TASK_NAME_2,
          ImmutableMap.of(TEST_STORE, ImmutableSet.of(STANDBY_CHANGELOG_SSP), SIDE_INPUT_STORE, STANDBY_TASK_INPUT_SSP));
    }
  }
}
