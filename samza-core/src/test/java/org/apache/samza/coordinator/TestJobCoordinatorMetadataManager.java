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
package org.apache.samza.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetJobCoordinatorMetadataMessage;
import org.apache.samza.job.JobCoordinatorMetadata;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.Serde;
import org.junit.Before;
import org.junit.Test;

import static org.apache.samza.coordinator.JobCoordinatorMetadataManager.ClusterType;
import static org.apache.samza.coordinator.JobCoordinatorMetadataManager.CONTAINER_ID_DELIMITER;
import static org.apache.samza.coordinator.JobCoordinatorMetadataManager.CONTAINER_ID_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * A test class for {@link JobCoordinatorMetadataManager}
 */
public class TestJobCoordinatorMetadataManager {
  private static final String OLD_CONFIG_ID = "1";
  private static final String OLD_JOB_MODEL_ID = "1";
  private static final String OLD_EPOCH_ID = "1606797336059" + CONTAINER_ID_DELIMITER + "0010";
  private static final String OLD_CONTAINER_ID = "CONTAINER" + CONTAINER_ID_DELIMITER + OLD_EPOCH_ID +
      CONTAINER_ID_DELIMITER + "00002";

  private static final String NEW_CONFIG_ID = "2";
  private static final String NEW_JOB_MODEL_ID = "2";
  private static final String NEW_EPOCH_ID = "1606797336059" + CONTAINER_ID_DELIMITER + "0011";

  private static final Config OLD_CONFIG = new MapConfig(
      ImmutableMap.of(
          "job.autosizing.enabled", "true",
          "job.autosizing.cpu.core", "16"));

  private static final Config NEW_CONFIG = new MapConfig(
      ImmutableMap.of(
          "job.autosizing.enabled", "true",
          "job.autosizing.cpu.core", "24"));

  private static final Config COORDINATOR_STORE_CONFIG =
      new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));

  private JobCoordinatorMetadataManager jobCoordinatorMetadataManager;
  private Map<String, ContainerModel> containerModelMap;
  private MetadataStore metadataStore;

  @Before
  public void setup() {
    Map<TaskName, TaskModel> tasksForContainer1 = ImmutableMap.of(
        new TaskName("t1"), new TaskModel(new TaskName("t1"), ImmutableSet.of(), new Partition(0)),
        new TaskName("t2"), new TaskModel(new TaskName("t2"), ImmutableSet.of(), new Partition(1)));
    Map<TaskName, TaskModel> tasksForContainer2 = ImmutableMap.of(
        new TaskName("t3"), new TaskModel(new TaskName("t3"), ImmutableSet.of(), new Partition(2)),
        new TaskName("t4"), new TaskModel(new TaskName("t4"), ImmutableSet.of(), new Partition(3)),
        new TaskName("t5"), new TaskModel(new TaskName("t5"), ImmutableSet.of(), new Partition(4)));
    ContainerModel containerModel1 = new ContainerModel("0", tasksForContainer1);
    ContainerModel containerModel2 = new ContainerModel("1", tasksForContainer2);
    containerModelMap = ImmutableMap.of("0", containerModel1, "1", containerModel2);
    CoordinatorStreamStoreTestUtil mockCoordinatorStreamStore =
        new CoordinatorStreamStoreTestUtil(COORDINATOR_STORE_CONFIG);
    metadataStore = spy(new NamespaceAwareCoordinatorStreamStore(
        mockCoordinatorStreamStore.getCoordinatorStreamStore(), SetJobCoordinatorMetadataMessage.TYPE));
    jobCoordinatorMetadataManager = spy(new JobCoordinatorMetadataManager(metadataStore,
        ClusterType.YARN, new MetricsRegistryMap()));
  }

  @Test
  public void testCheckForMetadataChanges() {
    JobCoordinatorMetadata previousMetadata = new JobCoordinatorMetadata(OLD_EPOCH_ID, OLD_CONFIG_ID, OLD_JOB_MODEL_ID);
    JobCoordinatorMetadata newMetadataWithDifferentEpochId =
        new JobCoordinatorMetadata(NEW_EPOCH_ID, OLD_CONFIG_ID, OLD_JOB_MODEL_ID);

    JobCoordinatorMetadataManager.JobCoordinatorMetadataManagerMetrics metrics =
        jobCoordinatorMetadataManager.getMetrics();

    boolean metadataChanged =
        jobCoordinatorMetadataManager.checkForMetadataChanges(previousMetadata, newMetadataWithDifferentEpochId);
    assertTrue("Metadata check should return true", metadataChanged);
    assertEquals("New deployment should be 1 since Epoch ID changed", 1,
        metrics.getNewDeployment().getValue().intValue());

    JobCoordinatorMetadata newMetadataWithDifferentConfigId =
        new JobCoordinatorMetadata(OLD_EPOCH_ID, NEW_CONFIG_ID, OLD_JOB_MODEL_ID);
    metadataChanged =
        jobCoordinatorMetadataManager.checkForMetadataChanges(previousMetadata, newMetadataWithDifferentConfigId);
    assertTrue("Metadata check should return true", metadataChanged);
    assertEquals("Config across application attempts should be 1", 1,
        metrics.getConfigChangedAcrossApplicationAttempt().getValue().intValue());

    JobCoordinatorMetadata newMetadataWithDifferentJobModelId =
        new JobCoordinatorMetadata(OLD_EPOCH_ID, OLD_CONFIG_ID, NEW_JOB_MODEL_ID);
    metadataChanged =
        jobCoordinatorMetadataManager.checkForMetadataChanges(previousMetadata, newMetadataWithDifferentJobModelId);
    assertTrue("Metadata check should return true", metadataChanged);
    assertEquals("Job model changed across application attempts should be 1", 1,
        metrics.getJobModelChangedAcrossApplicationAttempt().getValue().intValue());

    JobCoordinatorMetadata newMetadataWithNoChange =
        new JobCoordinatorMetadata(OLD_EPOCH_ID, OLD_CONFIG_ID, OLD_JOB_MODEL_ID);
    assertEquals("Application attempt count should be 0", 0,
        metrics.getApplicationAttemptCount().getCount());

    metadataChanged =
        jobCoordinatorMetadataManager.checkForMetadataChanges(previousMetadata, newMetadataWithNoChange);
    assertFalse("Metadata check should return false", metadataChanged);
    assertEquals("Application attempt count should be 1", 1,
        metrics.getApplicationAttemptCount().getCount());
  }

  @Test
  public void testGenerateJobCoordinatorMetadataFailed() {
    doThrow(new RuntimeException("Failed to generate epoch id"))
        .when(jobCoordinatorMetadataManager).fetchEpochIdForJobCoordinator();

    try {
      jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(new JobModel(OLD_CONFIG, containerModelMap), OLD_CONFIG);
      fail("Expected generate job coordinator metadata to throw exception");
    } catch (Exception e) {
      assertTrue("Expecting SamzaException to be thrown", e instanceof SamzaException);
      assertEquals("Metadata generation failed count should be 1", 1,
          jobCoordinatorMetadataManager.getMetrics().getMetadataGenerationFailedCount().getCount());
    }
  }

  @Test
  public void testGenerateJobCoordinatorMetadataForRepeatability() {
    when(jobCoordinatorMetadataManager.getEnvProperty(CONTAINER_ID_PROPERTY))
        .thenReturn(OLD_CONTAINER_ID);
    JobCoordinatorMetadata expectedMetadata = jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(
        new JobModel(OLD_CONFIG, containerModelMap), OLD_CONFIG);

    assertEquals("Mismatch in epoch identifier.", OLD_EPOCH_ID, expectedMetadata.getEpochId());

    JobCoordinatorMetadata actualMetadata = jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(
        new JobModel(OLD_CONFIG, containerModelMap), OLD_CONFIG);
    assertEquals("Expected repeatable job coordinator metadata", expectedMetadata, actualMetadata);
  }

  @Test
  public void testGenerateJobCoordinatorMetadataWithConfigChanges() {
    when(jobCoordinatorMetadataManager.getEnvProperty(CONTAINER_ID_PROPERTY))
        .thenReturn(OLD_CONTAINER_ID);
    JobCoordinatorMetadata expectedMetadata = jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(
        new JobModel(OLD_CONFIG, containerModelMap), OLD_CONFIG);

    Map<String, String> additionalConfig = new HashMap<>();
    additionalConfig.put("yarn.am.high-availability.enabled", "true");

    additionalConfig.putAll(OLD_CONFIG);
    Config modifiedConfig = new MapConfig(additionalConfig);
    JobCoordinatorMetadata actualMetadata = jobCoordinatorMetadataManager.generateJobCoordinatorMetadata(
        new JobModel(modifiedConfig, containerModelMap), modifiedConfig);
    assertEquals("Job coordinator metadata should remain the same", expectedMetadata, actualMetadata);
  }

  @Test
  public void testReadJobCoordinatorMetadataFailed() {
    JobCoordinatorMetadata jobCoordinatorMetadata =
        new JobCoordinatorMetadata(NEW_EPOCH_ID, NEW_CONFIG_ID, NEW_JOB_MODEL_ID);
    Serde<String> mockSerde = spy(new CoordinatorStreamValueSerde(SetJobCoordinatorMetadataMessage.TYPE));
    doThrow(new RuntimeException("Failed to read coordinator stream"))
        .when(mockSerde).fromBytes(any());

    jobCoordinatorMetadataManager = spy(new JobCoordinatorMetadataManager(metadataStore,
        ClusterType.YARN, new MetricsRegistryMap(), mockSerde));
    jobCoordinatorMetadataManager.writeJobCoordinatorMetadata(jobCoordinatorMetadata);

    JobCoordinatorMetadata actualMetadata = jobCoordinatorMetadataManager.readJobCoordinatorMetadata();
    assertNull("Read failed should return null", actualMetadata);
    assertEquals("Metadata read failed count should be 1", 1,
        jobCoordinatorMetadataManager.getMetrics().getMetadataReadFailedCount().getCount());
  }

  @Test
  public void testReadWriteJobCoordinatorMetadata() {
    JobCoordinatorMetadata jobCoordinatorMetadata =
        new JobCoordinatorMetadata(NEW_EPOCH_ID, NEW_CONFIG_ID, NEW_JOB_MODEL_ID);

    jobCoordinatorMetadataManager.writeJobCoordinatorMetadata(jobCoordinatorMetadata);

    JobCoordinatorMetadata actualJobCoordinatorMetadata = jobCoordinatorMetadataManager.readJobCoordinatorMetadata();
    assertEquals("Mismatch in job coordinator metadata", jobCoordinatorMetadata, actualJobCoordinatorMetadata);
  }

  @Test (expected = NullPointerException.class)
  public void testWriteNullJobCoordinatorMetadataShouldThrowException() {
    jobCoordinatorMetadataManager.writeJobCoordinatorMetadata(null);
  }

  @Test
  public void testWriteJobCoordinatorMetadataBubblesException() {
    doThrow(new RuntimeException("Failed to write to coordinator stream"))
        .when(metadataStore).put(anyString(), any());
    try {
      jobCoordinatorMetadataManager.writeJobCoordinatorMetadata(mock(JobCoordinatorMetadata.class));
      fail("Expected write job coordinator metadata to throw exception");
    } catch (Exception e) {
      assertTrue("Expecting SamzaException to be thrown", e instanceof SamzaException);
      assertEquals("Metadata write failed count should be 1", 1,
          jobCoordinatorMetadataManager.getMetrics().getMetadataWriteFailedCount().getCount());
    }
  }
}
