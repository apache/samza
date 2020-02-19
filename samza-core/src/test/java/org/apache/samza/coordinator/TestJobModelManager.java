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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.*;

import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.RegExTopicGenerator;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.GroupByContainerCount;
import org.apache.samza.container.grouper.task.GrouperMetadata;
import org.apache.samza.container.grouper.task.GrouperMetadataImpl;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.container.grouper.task.TaskPartitionAssignmentManager;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.HostLocality;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.testUtils.MockHttpServer;
import org.apache.samza.util.ConfigUtil;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.collection.JavaConversions;

/**
 * Unit tests for {@link JobModelManager}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TaskAssignmentManager.class, GroupByContainerCount.class, ConfigUtil.class})
public class TestJobModelManager {
  private final TaskAssignmentManager mockTaskManager = mock(TaskAssignmentManager.class);
  private final Map<String, Map<String, String>> localityMappings = new HashMap<>();
  private final HttpServer server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));
  private final SystemStream inputStream = new SystemStream("test-system", "test-stream");
  private final SystemStreamMetadata.SystemStreamPartitionMetadata mockSspMetadata = mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class);
  private final Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> mockSspMetadataMap = Collections.singletonMap(new Partition(0), mockSspMetadata);
  private final SystemStreamMetadata mockStreamMetadata = mock(SystemStreamMetadata.class);
  private final scala.collection.immutable.Map<SystemStream, SystemStreamMetadata> mockStreamMetadataMap = new scala.collection.immutable.Map.Map1(inputStream, mockStreamMetadata);
  private final StreamMetadataCache mockStreamMetadataCache = mock(StreamMetadataCache.class);
  private final scala.collection.immutable.Set<SystemStream> inputStreamSet = JavaConversions.asScalaSet(Collections.singleton(inputStream)).toSet();

  private JobModelManager jobModelManager;

  @Before
  public void setup() throws Exception {
    when(mockStreamMetadataCache.getStreamMetadata(argThat(new ArgumentMatcher<scala.collection.immutable.Set<SystemStream>>() {
      @Override
      public boolean matches(Object argument) {
        scala.collection.immutable.Set<SystemStream> set = (scala.collection.immutable.Set<SystemStream>) argument;
        return set.equals(inputStreamSet);
      }
    }), anyBoolean())).thenReturn(mockStreamMetadataMap);
    when(mockStreamMetadata.getSystemStreamPartitionMetadata()).thenReturn(mockSspMetadataMap);
    PowerMockito.whenNew(TaskAssignmentManager.class).withAnyArguments().thenReturn(mockTaskManager);
    when(mockTaskManager.readTaskAssignment()).thenReturn(Collections.EMPTY_MAP);
  }

  @Test
  public void testGetGrouperMetadata() {
    // Mocking setup.
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    TaskAssignmentManager mockTaskAssignmentManager = Mockito.mock(TaskAssignmentManager.class);
    TaskPartitionAssignmentManager mockTaskPartitionAssignmentManager = Mockito.mock(TaskPartitionAssignmentManager.class);

    SystemStreamPartition testSystemStreamPartition1 = new SystemStreamPartition(new SystemStream("test-system-0", "test-stream-0"), new Partition(1));
    SystemStreamPartition testSystemStreamPartition2 = new SystemStreamPartition(new SystemStream("test-system-1", "test-stream-1"), new Partition(2));

    when(mockLocalityManager.readLocality()).thenReturn(new LocalityModel(ImmutableMap.of("0", new HostLocality("0", "abc-affinity"))));

    Map<SystemStreamPartition, List<String>> taskToSSPAssignments = ImmutableMap.of(testSystemStreamPartition1, ImmutableList.of("task-0", "task-1"),
                                                                                    testSystemStreamPartition2, ImmutableList.of("task-2", "task-3"));

    Map<String, String> taskAssignment = ImmutableMap.of("task-0", "0");

    // Mock the task to partition assignment.
    when(mockTaskPartitionAssignmentManager.readTaskPartitionAssignments()).thenReturn(taskToSSPAssignments);

    // Mock the container to task assignment.
    when(mockTaskAssignmentManager.readTaskAssignment()).thenReturn(taskAssignment);
    when(mockTaskAssignmentManager.readTaskModes()).thenReturn(ImmutableMap.of(new TaskName("task-0"), TaskMode.Active, new TaskName("task-1"), TaskMode.Active, new TaskName("task-2"), TaskMode.Active, new TaskName("task-3"), TaskMode.Active));

    GrouperMetadataImpl grouperMetadata = JobModelManager.getGrouperMetadata(new MapConfig(), mockLocalityManager, mockTaskAssignmentManager, mockTaskPartitionAssignmentManager);

    verify(mockLocalityManager).readLocality();
    verify(mockTaskAssignmentManager).readTaskAssignment();

    Assert.assertEquals(ImmutableMap.of("0", new LocationId("abc-affinity")), grouperMetadata.getProcessorLocality());
    Assert.assertEquals(ImmutableMap.of(new TaskName("task-0"), new LocationId("abc-affinity")), grouperMetadata.getTaskLocality());

    Map<TaskName, List<SystemStreamPartition>> expectedTaskToSSPAssignments = ImmutableMap.of(new TaskName("task-0"), ImmutableList.of(testSystemStreamPartition1),
                                                                                              new TaskName("task-1"), ImmutableList.of(testSystemStreamPartition1),
                                                                                              new TaskName("task-2"), ImmutableList.of(testSystemStreamPartition2),
                                                                                              new TaskName("task-3"), ImmutableList.of(testSystemStreamPartition2));
    Assert.assertEquals(expectedTaskToSSPAssignments, grouperMetadata.getPreviousTaskToSSPAssignment());
  }

  @Test
  public void testGetProcessorLocalityAllEntriesExisting() {
    Config config = new MapConfig(ImmutableMap.of(JobConfig.JOB_CONTAINER_COUNT, "2"));

    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readLocality()).thenReturn(new LocalityModel(ImmutableMap.of(
        "0", new HostLocality("0", "0-affinity"),
        "1", new HostLocality("1", "1-affinity"))));

    Map<String, LocationId> processorLocality = JobModelManager.getProcessorLocality(config, mockLocalityManager);

    verify(mockLocalityManager).readLocality();
    ImmutableMap<String, LocationId> expected =
        ImmutableMap.of("0", new LocationId("0-affinity"), "1", new LocationId("1-affinity"));
    Assert.assertEquals(expected, processorLocality);
  }

  @Test
  public void testGetProcessorLocalityNewContainer() {
    Config config = new MapConfig(ImmutableMap.of(JobConfig.JOB_CONTAINER_COUNT, "2"));

    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    // 2 containers, but only return 1 existing mapping
    when(mockLocalityManager.readLocality()).thenReturn(new LocalityModel(ImmutableMap.of("0", new HostLocality("0", "abc-affinity"))));

    Map<String, LocationId> processorLocality = JobModelManager.getProcessorLocality(config, mockLocalityManager);

    verify(mockLocalityManager).readLocality();
    ImmutableMap<String, LocationId> expected = ImmutableMap.of(
        // found entry in existing locality
        "0", new LocationId("abc-affinity"),
        // no entry in existing locality
        "1", new LocationId("ANY_HOST"));
    Assert.assertEquals(expected, processorLocality);
  }

  @Test
  public void testUpdateTaskAssignments() {
    // Mocking setup.
    JobModel mockJobModel = Mockito.mock(JobModel.class);
    GrouperMetadataImpl mockGrouperMetadata = Mockito.mock(GrouperMetadataImpl.class);
    TaskAssignmentManager mockTaskAssignmentManager = Mockito.mock(TaskAssignmentManager.class);
    TaskPartitionAssignmentManager mockTaskPartitionAssignmentManager = Mockito.mock(TaskPartitionAssignmentManager.class);

    SystemStreamPartition testSystemStreamPartition1 = new SystemStreamPartition(new SystemStream("test-system-0", "test-stream-0"), new Partition(1));
    SystemStreamPartition testSystemStreamPartition2 = new SystemStreamPartition(new SystemStream("test-system-1", "test-stream-1"), new Partition(2));
    SystemStreamPartition testSystemStreamPartition3 = new SystemStreamPartition(new SystemStream("test-system-2", "test-stream-2"), new Partition(1));
    SystemStreamPartition testSystemStreamPartition4 = new SystemStreamPartition(new SystemStream("test-system-3", "test-stream-3"), new Partition(2));

    Map<TaskName, TaskModel> taskModelMap = new HashMap<>();
    taskModelMap.put(new TaskName("task-1"), new TaskModel(new TaskName("task-1"), ImmutableSet.of(testSystemStreamPartition1), new Partition(0)));
    taskModelMap.put(new TaskName("task-2"), new TaskModel(new TaskName("task-2"), ImmutableSet.of(testSystemStreamPartition2), new Partition(1)));
    taskModelMap.put(new TaskName("task-3"), new TaskModel(new TaskName("task-3"), ImmutableSet.of(testSystemStreamPartition3), new Partition(2)));
    taskModelMap.put(new TaskName("task-4"), new TaskModel(new TaskName("task-4"), ImmutableSet.of(testSystemStreamPartition4), new Partition(3)));
    ContainerModel containerModel = new ContainerModel("test-container-id", taskModelMap);
    Map<String, ContainerModel> containerMapping = ImmutableMap.of("test-container-id", containerModel);

    when(mockJobModel.getContainers()).thenReturn(containerMapping);
    when(mockGrouperMetadata.getPreviousTaskToProcessorAssignment()).thenReturn(new HashMap<>());
    Mockito.doNothing().when(mockTaskAssignmentManager).writeTaskContainerMappings(Mockito.any());

    JobModelManager.updateTaskAssignments(mockJobModel, mockTaskAssignmentManager, mockTaskPartitionAssignmentManager, mockGrouperMetadata);

    Set<String> taskNames = new HashSet<String>();
    taskNames.add("task-4");
    taskNames.add("task-2");
    taskNames.add("task-3");
    taskNames.add("task-1");

    Set<SystemStreamPartition> systemStreamPartitions = new HashSet<>();
    systemStreamPartitions.add(new SystemStreamPartition(new SystemStream("test-system-0", "test-stream-0"), new Partition(1)));
    systemStreamPartitions.add(new SystemStreamPartition(new SystemStream("test-system-1", "test-stream-1"), new Partition(2)));
    systemStreamPartitions.add(new SystemStreamPartition(new SystemStream("test-system-2", "test-stream-2"), new Partition(1)));
    systemStreamPartitions.add(new SystemStreamPartition(new SystemStream("test-system-3", "test-stream-3"), new Partition(2)));

    // Verifications
    verify(mockJobModel, atLeast(1)).getContainers();
    verify(mockTaskAssignmentManager).deleteTaskContainerMappings(Mockito.any());
    verify(mockTaskAssignmentManager).writeTaskContainerMappings(ImmutableMap.of("test-container-id",
        ImmutableMap.of("task-1", TaskMode.Active, "task-2", TaskMode.Active, "task-3", TaskMode.Active, "task-4", TaskMode.Active)));

    // Verify that the old, stale partition mappings had been purged in the coordinator stream.
    verify(mockTaskPartitionAssignmentManager).delete(systemStreamPartitions);

    // Verify that the new task to partition assignment is stored in the coordinator stream.
    verify(mockTaskPartitionAssignmentManager).writeTaskPartitionAssignments(ImmutableMap.of(
        testSystemStreamPartition1, ImmutableList.of("task-1"),
        testSystemStreamPartition2, ImmutableList.of("task-2"),
        testSystemStreamPartition3, ImmutableList.of("task-3"),
        testSystemStreamPartition4, ImmutableList.of("task-4")
    ));
  }

  @Test
  public void testJobModelContainsLatestTaskInputsWhenEnabledRegexTopicRewriter() {
    ImmutableMap<String, String> rewriterConfig = ImmutableMap.of(
        JobConfig.CONFIG_REWRITERS, "regexTopicRewriter",
        String.format(JobConfig.CONFIG_REWRITER_CLASS, "regexTopicRewriter"), RegExTopicGenerator.class.getCanonicalName()
    );

    Config config = new MapConfig(rewriterConfig);
    String taskInputMatchedRegex = inputStream.getSystem() + "." + inputStream.getStream();
    Config refreshedConfig = new MapConfig(ImmutableMap.<String, String>builder()
        .putAll(rewriterConfig)
        .put(TaskConfig.INPUT_STREAMS, taskInputMatchedRegex)
        .build()
    );

    PowerMockito.mockStatic(ConfigUtil.class);
    PowerMockito.when(ConfigUtil.applyRewriter(config, "regexTopicRewriter")).thenReturn(refreshedConfig);

    Map<TaskName, Integer> changeLogPartitionMapping = new HashMap<>();
    GrouperMetadata grouperMetadata = mock(GrouperMetadata.class);

    JobModel jobModel =
        JobModelManager.readJobModel(config, changeLogPartitionMapping, mockStreamMetadataCache, grouperMetadata);

    Assert.assertNotNull(jobModel);
    Assert.assertEquals(taskInputMatchedRegex, jobModel.getConfig().get(TaskConfig.INPUT_STREAMS));
  }
}
