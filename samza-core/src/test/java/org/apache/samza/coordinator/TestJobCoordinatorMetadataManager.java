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
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetJobCoordinatorMetadataMessage;
import org.apache.samza.job.JobCoordinatorMetadata;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.junit.Before;
import org.junit.Test;

import static org.apache.samza.coordinator.JobCoordinatorMetadataManager.CONTAINER_ID_DELIMITER;
import static org.apache.samza.coordinator.JobCoordinatorMetadataManager.CONTAINER_ID_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


/**
 * A test class for {@link JobCoordinatorMetadataManager}
 */
public class TestJobCoordinatorMetadataManager {
  private static final String CLUSTER_TYPE = "YARN";
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
    jobCoordinatorMetadataManager = spy(new JobCoordinatorMetadataManager(
        new NamespaceAwareCoordinatorStreamStore(mockCoordinatorStreamStore.getCoordinatorStreamStore(),
            SetJobCoordinatorMetadataMessage.TYPE), CLUSTER_TYPE, new MetricsRegistryMap()));
  }

  @Test
  public void testCheckForMetadataChanges() {
    JobCoordinatorMetadata previousMetadata = new JobCoordinatorMetadata(OLD_EPOCH_ID, OLD_CONFIG_ID, OLD_JOB_MODEL_ID);
    JobCoordinatorMetadata newMetadataWithDifferentEpochId =
        new JobCoordinatorMetadata(NEW_EPOCH_ID, OLD_CONFIG_ID, OLD_JOB_MODEL_ID);

    boolean metadataChanged =
        jobCoordinatorMetadataManager.checkForMetadataChanges(previousMetadata, newMetadataWithDifferentEpochId);
    assertTrue("Metadata check should return true", metadataChanged);
    assertEquals("New deployment should be 1 since Epoch ID changed", 1,
        jobCoordinatorMetadataManager.getNewDeployment().getValue().intValue());

    JobCoordinatorMetadata newMetadataWithDifferentConfigId =
        new JobCoordinatorMetadata(OLD_EPOCH_ID, NEW_CONFIG_ID, OLD_JOB_MODEL_ID);
    metadataChanged =
        jobCoordinatorMetadataManager.checkForMetadataChanges(previousMetadata, newMetadataWithDifferentConfigId);
    assertTrue("Metadata check should return true", metadataChanged);
    assertEquals("Config across application attempts should be 1", 1,
        jobCoordinatorMetadataManager.getConfigChangedAcrossApplicationAttempt().getValue().intValue());

    JobCoordinatorMetadata newMetadataWithDifferentJobModelId =
        new JobCoordinatorMetadata(OLD_EPOCH_ID, OLD_CONFIG_ID, NEW_JOB_MODEL_ID);
    metadataChanged =
        jobCoordinatorMetadataManager.checkForMetadataChanges(previousMetadata, newMetadataWithDifferentJobModelId);
    assertTrue("Metadata check should return true", metadataChanged);
    assertEquals("Job model changed across application attempts should be 1", 1,
        jobCoordinatorMetadataManager.getJobModelChangedAcrossApplicationAttempt().getValue().intValue());

    JobCoordinatorMetadata newMetadataWithNoChange =
        new JobCoordinatorMetadata(OLD_EPOCH_ID, OLD_CONFIG_ID, OLD_JOB_MODEL_ID);
    assertEquals("Application attempt count should be 0", 0,
        jobCoordinatorMetadataManager.getApplicationAttemptCount().getCount());

    metadataChanged =
        jobCoordinatorMetadataManager.checkForMetadataChanges(previousMetadata, newMetadataWithNoChange);
    assertFalse("Metadata check should return false", metadataChanged);
    assertEquals("Application attempt count should be 1", 1,
        jobCoordinatorMetadataManager.getApplicationAttemptCount().getCount());
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
  public void testReadWriteJobCoordinatorMetadata() {
    JobCoordinatorMetadata jobCoordinatorMetadata =
        new JobCoordinatorMetadata(NEW_EPOCH_ID, NEW_CONFIG_ID, NEW_JOB_MODEL_ID);

    boolean writeSucceeded = jobCoordinatorMetadataManager.writeJobCoordinatorMetadata(jobCoordinatorMetadata);
    assertTrue("Job coordinator metadata write failed", writeSucceeded);

    JobCoordinatorMetadata actualJobCoordinatorMetadata = jobCoordinatorMetadataManager.readJobCoordinatorMetadata();
    assertEquals("Mismatch in job coordinator metadata", jobCoordinatorMetadata, actualJobCoordinatorMetadata);
  }

  @Test (expected = NullPointerException.class)
  public void testWriteNullJobCoordinatorMetadataShouldThrowException() {
    jobCoordinatorMetadataManager.writeJobCoordinatorMetadata(null);
  }
}
