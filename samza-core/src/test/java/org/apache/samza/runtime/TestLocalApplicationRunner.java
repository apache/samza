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

package org.apache.samza.runtime;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.coordinator.ClusterMembership;
import org.apache.samza.coordinator.CoordinationConstants;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.DistributedLock;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamMetadataStoreFactory;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.execution.LocalJobPlanner;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metadatastore.InMemoryMetadataStore;
import org.apache.samza.metadatastore.InMemoryMetadataStoreFactory;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.task.IdentityStreamTask;
import org.apache.samza.zk.ZkMetadataStore;
import org.apache.samza.zk.ZkMetadataStoreFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LocalJobPlanner.class, LocalApplicationRunner.class, ZkMetadataStoreFactory.class})
public class TestLocalApplicationRunner {

  private Config config;
  private SamzaApplication mockApp;
  private LocalApplicationRunner runner;
  private LocalJobPlanner localPlanner;
  private CoordinationUtils coordinationUtils;
  private ZkMetadataStore metadataStore;
  private ClusterMembership clusterMembership;

  @Before
  public void setUp() throws Exception {
    config = new MapConfig();
    mockApp = mock(StreamApplication.class);
    prepareTest();
  }

  @Test
  public void testRunStreamTask() throws Exception {
    final Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    cfgs.put(ApplicationConfig.APP_NAME, "test-app");
    cfgs.put(ApplicationConfig.APP_ID, "test-appId");
    config = new MapConfig(cfgs);
    mockApp = new LegacyTaskApplication(IdentityStreamTask.class.getName());
    prepareTest();

    StreamProcessor sp = mock(StreamProcessor.class);
    CoordinatorStreamStore metadataStore = mock(CoordinatorStreamStore.class);

    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i -> {
      ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
      listener.afterStart();
      listener.afterStop();
      return null;
    }).when(sp).start();

    ExternalContext externalContext = mock(ExternalContext.class);
    doReturn(sp).when(runner)
        .createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.of(externalContext)), any(
            CoordinatorStreamStore.class));
    doReturn(metadataStore).when(runner).createCoordinatorStreamStore(any(Config.class));
    doReturn(ApplicationStatus.SuccessfulFinish).when(runner).status();

    runner.run(externalContext);

    verify(metadataStore).init();
    verify(metadataStore).close();

    assertEquals(ApplicationStatus.SuccessfulFinish, runner.status());
  }

  @Test
  public void testRunStreamTaskWithoutExternalContext() throws Exception {
    final Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    cfgs.put(ApplicationConfig.APP_NAME, "test-app");
    cfgs.put(ApplicationConfig.APP_ID, "test-appId");
    config = new MapConfig(cfgs);
    mockApp = new LegacyTaskApplication(IdentityStreamTask.class.getName());
    prepareTest();

    StreamProcessor sp = mock(StreamProcessor.class);
    CoordinatorStreamStore metadataStore = mock(CoordinatorStreamStore.class);

    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i -> {
      ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
      listener.afterStart();
      listener.afterStop();
      return null;
    }).when(sp).start();

    doReturn(sp).when(runner).createStreamProcessor(anyObject(), anyObject(),
        captor.capture(), eq(Optional.empty()), any(CoordinatorStreamStore.class));
    doReturn(metadataStore).when(runner).createCoordinatorStreamStore(any(Config.class));
    doReturn(ApplicationStatus.SuccessfulFinish).when(runner).status();

    runner.run();

    verify(metadataStore).init();
    verify(metadataStore).close();

    assertEquals(ApplicationStatus.SuccessfulFinish, runner.status());
  }

  @Test
  public void testRunComplete() throws Exception {
    Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    config = new MapConfig(cfgs);
    ProcessorLifecycleListenerFactory mockFactory = (pContext, cfg) -> mock(ProcessorLifecycleListener.class);
    mockApp = (StreamApplication) appDesc -> appDesc.withProcessorLifecycleListenerFactory(mockFactory);
    prepareTest();

    // return the jobConfigs from the planner
    doReturn(Collections.singletonList(new JobConfig(new MapConfig(config)))).when(localPlanner).prepareJobs();

    StreamProcessor sp = mock(StreamProcessor.class);
    CoordinatorStreamStore coordinatorStreamStore = mock(CoordinatorStreamStore.class);
    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i -> {
      ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
      listener.afterStart();
      listener.afterStop();
      return null;
    }).when(sp).start();

    ExternalContext externalContext = mock(ExternalContext.class);
    doReturn(sp).when(runner)
        .createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.of(externalContext)), any(CoordinatorStreamStore.class));
    doReturn(coordinatorStreamStore).when(runner).createCoordinatorStreamStore(any(Config.class));

    runner.run(externalContext);
    runner.waitForFinish();

    verify(coordinatorStreamStore).init();
    verify(coordinatorStreamStore).close();

    assertEquals(runner.status(), ApplicationStatus.SuccessfulFinish);
  }

  @Test
  public void testRunCompleteWithouCoordinatorStreamStore() throws Exception {
    Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    config = new MapConfig(cfgs);
    ProcessorLifecycleListenerFactory mockFactory = (pContext, cfg) -> mock(ProcessorLifecycleListener.class);
    mockApp = (StreamApplication) appDesc -> appDesc.withProcessorLifecycleListenerFactory(mockFactory);
    prepareTest();

    // return the jobConfigs from the planner
    doReturn(Collections.singletonList(new JobConfig(new MapConfig(config)))).when(localPlanner).prepareJobs();

    StreamProcessor sp = mock(StreamProcessor.class);
    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i -> {
      ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
      listener.afterStart();
      listener.afterStop();
      return null;
    }).when(sp).start();

    ExternalContext externalContext = mock(ExternalContext.class);
    doReturn(sp).when(runner)
        .createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.of(externalContext)), eq(null));
    doReturn(null).when(runner).createCoordinatorStreamStore(any(Config.class));

    runner.run(externalContext);
    runner.waitForFinish();

    assertEquals(runner.status(), ApplicationStatus.SuccessfulFinish);
  }

  @Test
  public void testRunFailure() throws Exception {
    Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.PROCESSOR_ID, "0");
    config = new MapConfig(cfgs);
    ProcessorLifecycleListenerFactory mockFactory = (pContext, cfg) -> mock(ProcessorLifecycleListener.class);
    mockApp = (StreamApplication) appDesc -> appDesc.withProcessorLifecycleListenerFactory(mockFactory);
    prepareTest();

    // return the jobConfigs from the planner
    doReturn(Collections.singletonList(new JobConfig(new MapConfig(config)))).when(localPlanner).prepareJobs();

    StreamProcessor sp = mock(StreamProcessor.class);
    CoordinatorStreamStore coordinatorStreamStore = mock(CoordinatorStreamStore.class);
    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i -> {
      throw new Exception("test failure");
    }).when(sp).start();

    ExternalContext externalContext = mock(ExternalContext.class);
    doReturn(sp).when(runner)
        .createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.of(externalContext)), any(
            CoordinatorStreamStore.class));
    doReturn(coordinatorStreamStore).when(runner).createCoordinatorStreamStore(any(Config.class));

    try {
      runner.run(externalContext);
      runner.waitForFinish();
    } catch (Throwable th) {
      assertNotNull(th);
    }

    verify(coordinatorStreamStore).init();
    verify(coordinatorStreamStore, never()).close();

    assertEquals(runner.status(), ApplicationStatus.UnsuccessfulFinish);
  }

  @Test
  public void testKill() throws Exception {
    Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    config = new MapConfig(cfgs);
    ProcessorLifecycleListenerFactory mockFactory = (pContext, cfg) -> mock(ProcessorLifecycleListener.class);
    mockApp = (StreamApplication) appDesc -> appDesc.withProcessorLifecycleListenerFactory(mockFactory);
    prepareTest();

    // return the jobConfigs from the planner
    doReturn(Collections.singletonList(new JobConfig(new MapConfig(config)))).when(localPlanner).prepareJobs();

    StreamProcessor sp = mock(StreamProcessor.class);
    CoordinatorStreamStore coordinatorStreamStore = mock(CoordinatorStreamStore.class);
    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i -> {
      ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
      listener.afterStart();
      return null;
    }).when(sp).start();

    doAnswer(new Answer() {
      private int count = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (++count == 1) {
          ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
          listener.afterStop();
          return null;
        }
        return null;
      }
    }).when(sp).stop();

    ExternalContext externalContext = mock(ExternalContext.class);
    doReturn(sp).when(runner)
        .createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.of(externalContext)), any(CoordinatorStreamStore.class));
    doReturn(coordinatorStreamStore).when(runner).createCoordinatorStreamStore(any(Config.class));

    runner.run(externalContext);
    runner.kill();

    verify(coordinatorStreamStore).init();
    verify(coordinatorStreamStore, atLeastOnce()).close();

    assertEquals(runner.status(), ApplicationStatus.SuccessfulFinish);
  }

  @Test
  public void testKillWithoutCoordinatorStream() throws Exception {
    Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    config = new MapConfig(cfgs);
    ProcessorLifecycleListenerFactory mockFactory = (pContext, cfg) -> mock(ProcessorLifecycleListener.class);
    mockApp = (StreamApplication) appDesc -> appDesc.withProcessorLifecycleListenerFactory(mockFactory);
    prepareTest();

    // return the jobConfigs from the planner
    doReturn(Collections.singletonList(new JobConfig(new MapConfig(config)))).when(localPlanner).prepareJobs();

    StreamProcessor sp = mock(StreamProcessor.class);
    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i -> {
      ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
      listener.afterStart();
      return null;
    }).when(sp).start();

    doAnswer(new Answer() {
      private int count = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (++count == 1) {
          ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
          listener.afterStop();
          return null;
        }
        return null;
      }
    }).when(sp).stop();

    ExternalContext externalContext = mock(ExternalContext.class);
    doReturn(sp).when(runner)
        .createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.of(externalContext)), any(CoordinatorStreamStore.class));
    doReturn(null).when(runner).createCoordinatorStreamStore(any(Config.class));

    runner.run(externalContext);
    runner.kill();

    assertEquals(runner.status(), ApplicationStatus.SuccessfulFinish);
  }

  @Test
  public void testWaitForFinishReturnsBeforeTimeout() {
    long timeoutInMs = 1000;

    runner.getShutdownLatch().countDown();
    boolean finished = runner.waitForFinish(Duration.ofMillis(timeoutInMs));
    assertTrue("Application did not finish before the timeout.", finished);
  }

  @Test
  public void testWaitForFinishTimesout() {
    long timeoutInMs = 100;
    boolean finished = runner.waitForFinish(Duration.ofMillis(timeoutInMs));
    assertFalse("Application finished before the timeout.", finished);
  }

  @Test
  public void testCreateProcessorIdShouldReturnProcessorIdDefinedInConfiguration() {
    String processorId = "testProcessorId";
    MapConfig configMap = new MapConfig(ImmutableMap.of(ApplicationConfig.PROCESSOR_ID, processorId));
    String actualProcessorId = LocalApplicationRunner.createProcessorId(new ApplicationConfig(configMap));
    assertEquals(processorId, actualProcessorId);
  }

  @Test
  public void testCreateProcessorIdShouldInvokeProcessorIdGeneratorDefinedInConfiguration() {
    String processorId = "testProcessorId";
    MapConfig configMap = new MapConfig(ImmutableMap.of(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, MockProcessorIdGenerator.class.getCanonicalName()));
    String actualProcessorId = LocalApplicationRunner.createProcessorId(new ApplicationConfig(configMap));
    assertEquals(processorId, actualProcessorId);
  }

  @Test(expected = ConfigException.class)
  public void testCreateProcessorIdShouldThrowExceptionWhenProcessorIdAndGeneratorAreNotDefined() {
    ApplicationConfig mockConfig = Mockito.mock(ApplicationConfig.class);
    Mockito.when(mockConfig.getProcessorId()).thenReturn(null);
    LocalApplicationRunner.createProcessorId(mockConfig);
  }

  private void prepareTest() throws Exception {
    CoordinationUtils coordinationUtils = mock(CoordinationUtils.class);

    DistributedLock distributedLock = mock(DistributedLock.class);
    when(distributedLock.lock(anyObject())).thenReturn(true);
    when(coordinationUtils.getLock(anyString())).thenReturn(distributedLock);

    ZkMetadataStore zkMetadataStore = mock(ZkMetadataStore.class);
    when(zkMetadataStore.get(any())).thenReturn(null);
    PowerMockito.whenNew(ZkMetadataStore.class).withAnyArguments().thenReturn(zkMetadataStore);

    ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc =
        ApplicationDescriptorUtil.getAppDescriptor(mockApp, config);
    localPlanner = spy(new LocalJobPlanner(appDesc, coordinationUtils, "FAKE_UID", "FAKE_RUNID"));
    runner = spy(new LocalApplicationRunner(mockApp, config, coordinationUtils));
    doReturn(localPlanner).when(runner).getPlanner();
  }

  /**
   * For app.mode=BATCH ensure that the run.id generation utils --
   * DistributedLock, ClusterMembership and MetadataStore are created.
   * Also ensure that metadataStore.put is invoked (to write the run.id)
   */
  @Test
  public void testRunIdForBatch() throws Exception {
    final Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_MODE, "BATCH");
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    cfgs.put(JobConfig.JOB_NAME, "test-task-job");
    cfgs.put(JobConfig.JOB_ID, "jobId");
    config = new MapConfig(cfgs);
    mockApp = new LegacyTaskApplication(IdentityStreamTask.class.getName());

    prepareTestForRunId();
    runner.run();

    verify(coordinationUtils, Mockito.times(1)).getLock(CoordinationConstants.RUNID_LOCK_ID);
    verify(clusterMembership, Mockito.times(1)).getNumberOfProcessors();
    verify(metadataStore, Mockito.times(1)).put(eq(CoordinationConstants.RUNID_STORE_KEY), any(byte[].class));
    verify(metadataStore, Mockito.times(1)).flush();
  }

  /**
   * For app.mode=STREAM ensure that the run.id generation utils --
   * DistributedLock, ClusterMembership and MetadataStore are NOT created.
   * Also ensure that metadataStore.put is NOT invoked
   */
  @Test
  public void testRunIdForStream() throws Exception {
    final Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_MODE, "STREAM");
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    cfgs.put(JobConfig.JOB_NAME, "test-task-job");
    cfgs.put(JobConfig.JOB_ID, "jobId");
    config = new MapConfig(cfgs);
    mockApp = new LegacyTaskApplication(IdentityStreamTask.class.getName());

    prepareTestForRunId();

    runner.run();


    verify(coordinationUtils, Mockito.times(0)).getLock(CoordinationConstants.RUNID_LOCK_ID);
    verify(coordinationUtils, Mockito.times(0)).getClusterMembership();
    verify(clusterMembership, Mockito.times(0)).getNumberOfProcessors();
    verify(metadataStore, Mockito.times(0)).put(eq(CoordinationConstants.RUNID_STORE_KEY), any(byte[].class));
    verify(metadataStore, Mockito.times(0)).flush();
  }

  private void prepareTestForRunId() throws Exception {
    coordinationUtils = mock(CoordinationUtils.class);

    DistributedLock lock = mock(DistributedLock.class);
    when(lock.lock(anyObject())).thenReturn(true);
    when(coordinationUtils.getLock(anyString())).thenReturn(lock);

    clusterMembership = mock(ClusterMembership.class);
    when(clusterMembership.getNumberOfProcessors()).thenReturn(1);
    when(coordinationUtils.getClusterMembership()).thenReturn(clusterMembership);

    metadataStore = mock(ZkMetadataStore.class);
    when(metadataStore.get(any())).thenReturn(null);
    PowerMockito.whenNew(ZkMetadataStore.class).withAnyArguments().thenReturn(metadataStore);

    ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc =
        ApplicationDescriptorUtil.getAppDescriptor(mockApp, config);
    runner = spy(new LocalApplicationRunner(mockApp, config, coordinationUtils));
    localPlanner = spy(new LocalJobPlanner(appDesc, coordinationUtils, "FAKE_UID", "FAKE_RUNID"));
    doReturn(localPlanner).when(runner).getPlanner();
    StreamProcessor sp = mock(StreamProcessor.class);
    CoordinatorStreamStore coordinatorStreamStore = mock(CoordinatorStreamStore.class);
    doReturn(sp).when(runner).createStreamProcessor(anyObject(), anyObject(), anyObject(), anyObject(), any(
        CoordinatorStreamStore.class));
    doReturn(coordinatorStreamStore).when(runner).createCoordinatorStreamStore(any(Config.class));
  }

  /**
   * Default metadata store factory should be null if no job coordinator system defined and the default
   * ZkJobCoordinator is used.
   */
  @Test
  public void testGetCoordinatorStreamStoreFactoryWithoutJobCoordinatorSystem() {
    Optional<MetadataStoreFactory> metadataStoreFactory =
        LocalApplicationRunner.getDefaultCoordinatorStreamStoreFactory(new MapConfig());
    assertFalse(metadataStoreFactory.isPresent());
  }

  /**
   * Default metadata store factory should not be null if job coordinator system defined and the default
   * ZkJobCoordinator is used.
   */
  @Test
  public void testGetCoordinatorStreamStoreFactoryWithJobCoordinatorSystem() {
    Optional<MetadataStoreFactory> metadataStoreFactory =
        LocalApplicationRunner.getDefaultCoordinatorStreamStoreFactory(new MapConfig(ImmutableMap.of(JobConfig.JOB_COORDINATOR_SYSTEM, "test-system")));
    assertTrue(metadataStoreFactory.isPresent());
  }

  /**
   * Default metadata store factory should not be null if default system defined and the default
   * ZkJobCoordinator is used.
   */
  @Test
  public void testGetCoordinatorStreamStoreFactoryWithDefaultSystem() {
    Optional<MetadataStoreFactory> metadataStoreFactory =
        LocalApplicationRunner.getDefaultCoordinatorStreamStoreFactory(new MapConfig(ImmutableMap.of(JobConfig.JOB_DEFAULT_SYSTEM, "test-system")));
    assertTrue(metadataStoreFactory.isPresent());
  }

  /**
   * Default metadata store factory be null if job coordinator system or default system defined and a non ZkJobCoordinator
   * job coordinator is used.
   */
  @Test
  public void testGetCoordinatorStreamStoreFactoryWithNonZkJobCoordinator() {
    MapConfig mapConfig = new MapConfig(
        ImmutableMap.of(
            JobConfig.JOB_DEFAULT_SYSTEM, "test-system",
            JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName()));
    Optional<MetadataStoreFactory> metadataStoreFactory =
        LocalApplicationRunner.getDefaultCoordinatorStreamStoreFactory(mapConfig);
    assertFalse(metadataStoreFactory.isPresent());
  }

  /**
   * Underlying coordinator stream should be created if using CoordinatorStreamMetadataStoreFactory
   */
  @Test
  public void testCreateCoordinatorStreamWithCoordinatorFactory() throws Exception {
    CoordinatorStreamStore coordinatorStreamStore = mock(CoordinatorStreamStore.class);
    CoordinatorStreamMetadataStoreFactory coordinatorStreamMetadataStoreFactory = mock(CoordinatorStreamMetadataStoreFactory.class);
    doReturn(coordinatorStreamStore).when(coordinatorStreamMetadataStoreFactory).getMetadataStore(anyString(), any(Config.class), any(
        MetricsRegistry.class));
    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    PowerMockito.whenNew(SystemAdmins.class).withAnyArguments().thenReturn(systemAdmins);
    LocalApplicationRunner localApplicationRunner =
        spy(new LocalApplicationRunner(mockApp, config, coordinatorStreamMetadataStoreFactory));

    // create store only if successful in creating the underlying coordinator stream
    doReturn(true).when(localApplicationRunner).createUnderlyingCoordinatorStream(eq(config));
    assertEquals(coordinatorStreamStore, localApplicationRunner.createCoordinatorStreamStore(config));
    verify(localApplicationRunner).createUnderlyingCoordinatorStream(eq(config));

    // do not create store if creating the underlying coordinator stream fails
    doReturn(false).when(localApplicationRunner).createUnderlyingCoordinatorStream(eq(config));
    assertNull(localApplicationRunner.createCoordinatorStreamStore(config));
  }

  /**
   * Underlying coordinator stream should not be created if not using CoordinatorStreamMetadataStoreFactory
   */
  @Test
  public void testCreateCoordinatorStreamWithoutCoordinatorFactory() throws Exception {
    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    PowerMockito.whenNew(SystemAdmins.class).withAnyArguments().thenReturn(systemAdmins);
    LocalApplicationRunner localApplicationRunner =
        spy(new LocalApplicationRunner(mockApp, config, new InMemoryMetadataStoreFactory()));
    doReturn(false).when(localApplicationRunner).createUnderlyingCoordinatorStream(eq(config));
    MetadataStore coordinatorStreamStore = localApplicationRunner.createCoordinatorStreamStore(config);
    assertTrue(coordinatorStreamStore instanceof InMemoryMetadataStore);

    // creating underlying coordinator stream should not be called for other coordinator stream metadata store types.
    verify(localApplicationRunner, never()).createUnderlyingCoordinatorStream(eq(config));
  }
}
