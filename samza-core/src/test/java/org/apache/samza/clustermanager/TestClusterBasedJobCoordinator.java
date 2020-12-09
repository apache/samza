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

package org.apache.samza.clustermanager;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.JobCoordinatorMetadataManager;
import org.apache.samza.coordinator.StreamPartitionCountMonitor;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.execution.RemoteJobPlanner;
import org.apache.samza.job.JobCoordinatorMetadata;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.system.MockSystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.exceptions.base.MockitoException;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;


/**
 * Tests for {@link ClusterBasedJobCoordinator}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    CoordinatorStreamUtil.class,
    ClusterBasedJobCoordinatorRunner.class,
    CoordinatorStreamStore.class,
    RemoteJobPlanner.class})
public class TestClusterBasedJobCoordinator {

  private Map<String, String> configMap;

  @Before
  public void setUp() {
    configMap = new HashMap<>();
    configMap.put("job.name", "test-job");
    configMap.put("job.coordinator.system", "kafka");
    configMap.put("task.inputs", "kafka.topic1");
    configMap.put("systems.kafka.samza.factory", "org.apache.samza.system.MockSystemFactory");
    configMap.put("samza.cluster-manager.factory", "org.apache.samza.clustermanager.MockClusterResourceManagerFactory");
    configMap.put("job.coordinator.monitor-partition-change.frequency.ms", "1");

    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("kafka", "topic1", new Partition(0)), new ArrayList<>());
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("kafka", "__samza_coordinator_test-job_1", new Partition(0)), new ArrayList<>());
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache();
    PowerMockito.mockStatic(CoordinatorStreamUtil.class);
    when(CoordinatorStreamUtil.getCoordinatorSystemFactory(anyObject())).thenReturn(
        new MockCoordinatorStreamSystemFactory());
    when(CoordinatorStreamUtil.getCoordinatorSystemStream(anyObject())).thenReturn(new SystemStream("kafka", "test"));
    when(CoordinatorStreamUtil.getCoordinatorStreamName(anyObject(), anyObject())).thenReturn("test");
  }

  @After
  public void tearDown() {
    MockSystemFactory.MSG_QUEUES.clear();
  }

  @Test
  public void testPartitionCountMonitorWithDurableStates() {
    configMap.put("stores.mystore.changelog", "mychangelog");
    configMap.put(JobConfig.JOB_CONTAINER_COUNT, "1");
    when(CoordinatorStreamUtil.readConfigFromCoordinatorStream(anyObject())).thenReturn(new MapConfig(configMap));
    Config config = new MapConfig(configMap);

    // mimic job runner code to write the config to coordinator stream
    CoordinatorStreamSystemProducer producer = new CoordinatorStreamSystemProducer(config, mock(MetricsRegistry.class));
    producer.writeConfig("test-job", config);

    ClusterBasedJobCoordinator clusterCoordinator = ClusterBasedJobCoordinatorRunner.createFromMetadataStore(config);

    // change the input system stream metadata
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("kafka", "topic1", new Partition(1)), new ArrayList<>());

    StreamPartitionCountMonitor monitor = clusterCoordinator.getPartitionMonitor();
    monitor.updatePartitionCountMetric();
    assertEquals(clusterCoordinator.getAppStatus(), SamzaApplicationState.SamzaAppStatus.FAILED);
  }

  @Test
  public void testPartitionCountMonitorWithoutDurableStates() {
    configMap.put(JobConfig.JOB_CONTAINER_COUNT, "1");
    when(CoordinatorStreamUtil.readConfigFromCoordinatorStream(anyObject())).thenReturn(new MapConfig(configMap));
    Config config = new MapConfig(configMap);

    // mimic job runner code to write the config to coordinator stream
    CoordinatorStreamSystemProducer producer = new CoordinatorStreamSystemProducer(config, mock(MetricsRegistry.class));
    producer.writeConfig("test-job", config);

    ClusterBasedJobCoordinator clusterCoordinator = ClusterBasedJobCoordinatorRunner.createFromMetadataStore(config);

    // change the input system stream metadata
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("kafka", "topic1", new Partition(1)), new ArrayList<>());

    StreamPartitionCountMonitor monitor = clusterCoordinator.getPartitionMonitor();
    monitor.updatePartitionCountMetric();
    assertEquals(clusterCoordinator.getAppStatus(), SamzaApplicationState.SamzaAppStatus.UNDEFINED);
  }

  @Test
  public void testVerifyStartpointManagerFanOut() throws IOException {
    configMap.put(JobConfig.JOB_CONTAINER_COUNT, "1");
    configMap.put("job.jmx.enabled", "false");
    when(CoordinatorStreamUtil.readConfigFromCoordinatorStream(anyObject())).thenReturn(new MapConfig(configMap));
    Config config = new MapConfig(configMap);
    MockitoException stopException = new MockitoException("Stop");

    ClusterBasedJobCoordinator clusterCoordinator = spy(ClusterBasedJobCoordinatorRunner.createFromMetadataStore(config));
    ContainerProcessManager mockContainerProcessManager = mock(ContainerProcessManager.class);
    doReturn(true).when(mockContainerProcessManager).shouldShutdown();
    StartpointManager mockStartpointManager = mock(StartpointManager.class);

    // Stop ClusterBasedJobCoordinator#run after stop() method by throwing an exception to stop the run loop.
    // ClusterBasedJobCoordinator will need to be refactored for better mock support.
    doThrow(stopException).when(mockStartpointManager).stop();

    doReturn(mockContainerProcessManager).when(clusterCoordinator).createContainerProcessManager();
    doReturn(mockStartpointManager).when(clusterCoordinator).createStartpointManager();
    try {
      clusterCoordinator.run();
    } catch (SamzaException ex) {
      assertEquals(stopException, ex.getCause());
      verify(mockStartpointManager).start();
      verify(mockStartpointManager).fanOut(any());
      verify(mockStartpointManager).stop();
      return;
    }
    fail("Expected run() method to stop after StartpointManager#stop()");
  }

  @Test
  public void testVerifyShouldFanoutStartpointWithoutAMHA() {
    Config jobConfig = new MapConfig(configMap);

    when(CoordinatorStreamUtil.readConfigFromCoordinatorStream(anyObject())).thenReturn(jobConfig);
    ClusterBasedJobCoordinator clusterBasedJobCoordinator =
        spy(ClusterBasedJobCoordinatorRunner.createFromMetadataStore(jobConfig));

    when(clusterBasedJobCoordinator.isMetadataChangedAcrossAttempts()).thenReturn(true);
    assertTrue("Startpoint should fanout even if metadata changed",
        clusterBasedJobCoordinator.shouldFanoutStartpoint());

    when(clusterBasedJobCoordinator.isMetadataChangedAcrossAttempts()).thenReturn(false);
    assertTrue("Startpoint should fanout even if metadata remains unchanged",
        clusterBasedJobCoordinator.shouldFanoutStartpoint());
  }

  @Test
  public void testVerifyShouldFanoutStartpointWithAMHA() {
    Config jobConfig = new MapConfig(configMap);

    when(CoordinatorStreamUtil.readConfigFromCoordinatorStream(anyObject())).thenReturn(jobConfig);
    ClusterBasedJobCoordinator clusterBasedJobCoordinator =
        spy(ClusterBasedJobCoordinatorRunner.createFromMetadataStore(jobConfig));

    when(clusterBasedJobCoordinator.isApplicationMasterHighAvailabilityEnabled()).thenReturn(true);

    when(clusterBasedJobCoordinator.isMetadataChangedAcrossAttempts()).thenReturn(true);
    assertTrue("Startpoint should fanout with change in metadata",
        clusterBasedJobCoordinator.shouldFanoutStartpoint());

    when(clusterBasedJobCoordinator.isMetadataChangedAcrossAttempts()).thenReturn(false);
    assertFalse("Startpoint fan out shouldn't happen when metadata is unchanged",
        clusterBasedJobCoordinator.shouldFanoutStartpoint());

  }

  @Test
  public void testToArgs() {
    ApplicationConfig appConfig = new ApplicationConfig(new MapConfig(ImmutableMap.of(
        JobConfig.JOB_NAME, "test1",
        ApplicationConfig.APP_CLASS, "class1",
        ApplicationConfig.APP_MAIN_ARGS, "--runner=SamzaRunner --maxSourceParallelism=1024"
    )));

    List<String> expected = Arrays.asList(
        "--config", "job.name=test1",
        "--config", "app.class=class1",
        "--runner=SamzaRunner",
        "--maxSourceParallelism=1024");
    List<String> actual = Arrays.asList(ClusterBasedJobCoordinatorRunner.toArgs(appConfig));

    // cannot assert expected equals to actual as the order can be different.
    assertEquals(expected.size(), actual.size());
    assertTrue(actual.containsAll(expected));
  }

  @Test
  public void testGenerateAndUpdateJobCoordinatorMetadata() {
    Config jobConfig = new MapConfig(configMap);
    when(CoordinatorStreamUtil.readConfigFromCoordinatorStream(anyObject())).thenReturn(jobConfig);
    ClusterBasedJobCoordinator clusterBasedJobCoordinator =
        spy(ClusterBasedJobCoordinatorRunner.createFromMetadataStore(jobConfig));

    JobCoordinatorMetadata previousMetadata = mock(JobCoordinatorMetadata.class);
    JobCoordinatorMetadata newMetadata = mock(JobCoordinatorMetadata.class);
    JobCoordinatorMetadataManager jobCoordinatorMetadataManager = mock(JobCoordinatorMetadataManager.class);
    JobModel mockJobModel = mock(JobModel.class);

    when(jobCoordinatorMetadataManager.readJobCoordinatorMetadata()).thenReturn(previousMetadata);
    when(jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(any(), any())).thenReturn(newMetadata);
    when(jobCoordinatorMetadataManager.checkForMetadataChanges(newMetadata, previousMetadata)).thenReturn(false);
    when(clusterBasedJobCoordinator.createJobCoordinatorMetadataManager()).thenReturn(jobCoordinatorMetadataManager);

    /*
     * Verify if there are no changes to metadata, the metadata changed flag remains false and no interactions
     * with job coordinator metadata manager
     */
    clusterBasedJobCoordinator.generateAndUpdateJobCoordinatorMetadata(mockJobModel);
    assertFalse("JC metadata changed should remain unchanged",
        clusterBasedJobCoordinator.isMetadataChangedAcrossAttempts());
    verify(jobCoordinatorMetadataManager, times(0)).writeJobCoordinatorMetadata(any());

    /*
     * Verify if there are changes to metadata, we persist the new metadata & update the metadata changed flag
     */
    when(jobCoordinatorMetadataManager.checkForMetadataChanges(newMetadata, previousMetadata)).thenReturn(true);
    clusterBasedJobCoordinator.generateAndUpdateJobCoordinatorMetadata(mockJobModel);
    assertTrue("JC metadata changed should be true", clusterBasedJobCoordinator.isMetadataChangedAcrossAttempts());
    verify(jobCoordinatorMetadataManager, times(1)).writeJobCoordinatorMetadata(newMetadata);
  }
}
