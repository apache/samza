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
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.GroupByContainerCount;
import org.apache.samza.container.grouper.task.GrouperMetadataImpl;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.container.grouper.task.TaskPartitionAssignmentManager;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.testUtils.MockHttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

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
@PrepareForTest({TaskAssignmentManager.class, GroupByContainerCount.class})
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
  public void testLocalityMapWithHostAffinity() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        put("cluster-manager.container.count", "1");
        put("cluster-manager.container.memory.mb", "512");
        put("cluster-manager.container.retry.count", "1");
        put("cluster-manager.container.retry.window.ms", "1999999999");
        put("cluster-manager.allocator.sleep.ms", "10");
        put("yarn.package.path", "/foo");
        put("task.inputs", "test-system.test-stream");
        put("systems.test-system.samza.factory", "org.apache.samza.system.MockSystemFactory");
        put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
        put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
        put("job.host-affinity.enabled", "true");
      }
    });
    LocalityManager mockLocalityManager = mock(LocalityManager.class);

    localityMappings.put("0", new HashMap<String, String>() { {
        put(SetContainerHostMapping.HOST_KEY, "abc-affinity");
      } });
    when(mockLocalityManager.readContainerLocality()).thenReturn(this.localityMappings);

    Map<String, LocationId> containerLocality = ImmutableMap.of("0", new LocationId("abc-affinity"));
    this.jobModelManager = JobModelManagerTestUtil.getJobModelManagerUsingReadModel(config, mockStreamMetadataCache, server, mockLocalityManager, containerLocality);

    assertEquals(jobModelManager.jobModel().getAllContainerLocality(), new HashMap<String, String>() { { this.put("0", "abc-affinity"); } });
  }

  @Test
  public void testLocalityMapWithoutHostAffinity() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        put("cluster-manager.container.count", "1");
        put("cluster-manager.container.memory.mb", "512");
        put("cluster-manager.container.retry.count", "1");
        put("cluster-manager.container.retry.window.ms", "1999999999");
        put("cluster-manager.allocator.sleep.ms", "10");
        put("yarn.package.path", "/foo");
        put("task.inputs", "test-system.test-stream");
        put("systems.test-system.samza.factory", "org.apache.samza.system.MockSystemFactory");
        put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
        put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
        put("job.host-affinity.enabled", "false");
      }
    });

    LocalityManager mockLocalityManager = mock(LocalityManager.class);

    localityMappings.put("0", new HashMap<String, String>() { {
        put(SetContainerHostMapping.HOST_KEY, "abc-affinity");
      } });
    when(mockLocalityManager.readContainerLocality()).thenReturn(new HashMap<>());

    Map<String, LocationId> containerLocality = ImmutableMap.of("0", new LocationId("abc-affinity"));

    this.jobModelManager = JobModelManagerTestUtil.getJobModelManagerUsingReadModel(config, mockStreamMetadataCache, server, mockLocalityManager, containerLocality);

    assertEquals(jobModelManager.jobModel().getAllContainerLocality(), new HashMap<String, String>() { { this.put("0", null); } });
  }

  @Test
  public void testGetGrouperMetadata() {
    // Mocking setup.
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    TaskAssignmentManager mockTaskAssignmentManager = Mockito.mock(TaskAssignmentManager.class);
    TaskPartitionAssignmentManager mockTaskPartitionAssignmentManager = Mockito.mock(TaskPartitionAssignmentManager.class);

    SystemStreamPartition testSystemStreamPartition1 = new SystemStreamPartition(new SystemStream("test-system-0", "test-stream-0"), new Partition(1));
    SystemStreamPartition testSystemStreamPartition2 = new SystemStreamPartition(new SystemStream("test-system-1", "test-stream-1"), new Partition(2));

    Map<String, Map<String, String>> localityMappings = new HashMap<>();
    localityMappings.put("0", ImmutableMap.of(SetContainerHostMapping.HOST_KEY, "abc-affinity"));

    Map<SystemStreamPartition, List<String>> taskToSSPAssignments = ImmutableMap.of(testSystemStreamPartition1, ImmutableList.of("task-0", "task-1"),
                                                                                    testSystemStreamPartition2, ImmutableList.of("task-2", "task-3"));

    Map<String, String> taskAssignment = ImmutableMap.of("task-0", "0");

    // Mock the container locality assignment.
    when(mockLocalityManager.readContainerLocality()).thenReturn(localityMappings);

    // Mock the task to partition assignment.
    when(mockTaskPartitionAssignmentManager.readTaskPartitionAssignments()).thenReturn(taskToSSPAssignments);

    // Mock the container to task assignment.
    when(mockTaskAssignmentManager.readTaskAssignment()).thenReturn(taskAssignment);
    when(mockTaskAssignmentManager.readTaskModes()).thenReturn(ImmutableMap.of(new TaskName("task-0"), TaskMode.Active, new TaskName("task-1"), TaskMode.Active, new TaskName("task-2"), TaskMode.Active, new TaskName("task-3"), TaskMode.Active));

    GrouperMetadataImpl grouperMetadata = JobModelManager.getGrouperMetadata(new MapConfig(), mockLocalityManager, mockTaskAssignmentManager, mockTaskPartitionAssignmentManager);

    Mockito.verify(mockLocalityManager).readContainerLocality();
    Mockito.verify(mockTaskAssignmentManager).readTaskAssignment();

    Assert.assertEquals(ImmutableMap.of("0", new LocationId("abc-affinity"), "1", new LocationId("ANY_HOST")), grouperMetadata.getProcessorLocality());
    Assert.assertEquals(ImmutableMap.of(new TaskName("task-0"), new LocationId("abc-affinity")), grouperMetadata.getTaskLocality());

    Map<TaskName, List<SystemStreamPartition>> expectedTaskToSSPAssignments = ImmutableMap.of(new TaskName("task-0"), ImmutableList.of(testSystemStreamPartition1),
                                                                                              new TaskName("task-1"), ImmutableList.of(testSystemStreamPartition1),
                                                                                              new TaskName("task-2"), ImmutableList.of(testSystemStreamPartition2),
                                                                                              new TaskName("task-3"), ImmutableList.of(testSystemStreamPartition2));
    Assert.assertEquals(expectedTaskToSSPAssignments, grouperMetadata.getPreviousTaskToSSPAssignment());
  }

  @Test
  public void testGetProcessorLocality() {
    // Mock the dependencies.
    LocalityManager mockLocalityManager = mock(LocalityManager.class);

    Map<String, Map<String, String>> localityMappings = new HashMap<>();
    localityMappings.put("0", ImmutableMap.of(SetContainerHostMapping.HOST_KEY, "abc-affinity"));

    // Mock the container locality assignment.
    when(mockLocalityManager.readContainerLocality()).thenReturn(localityMappings);

    Map<String, LocationId> processorLocality = JobModelManager.getProcessorLocality(new MapConfig(), mockLocalityManager);

    Mockito.verify(mockLocalityManager).readContainerLocality();
    Assert.assertEquals(ImmutableMap.of("0", new LocationId("abc-affinity"), "1", new LocationId("ANY_HOST")), processorLocality);
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
    Mockito.doNothing().when(mockTaskAssignmentManager).writeTaskContainerMapping(Mockito.any(), Mockito.any(), Mockito.any());

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
    Mockito.verify(mockJobModel, atLeast(1)).getContainers();
    Mockito.verify(mockTaskAssignmentManager).deleteTaskContainerMappings(Mockito.any());
    Mockito.verify(mockTaskAssignmentManager).writeTaskContainerMapping("task-1", "test-container-id", TaskMode.Active);
    Mockito.verify(mockTaskAssignmentManager).writeTaskContainerMapping("task-2", "test-container-id", TaskMode.Active);
    Mockito.verify(mockTaskAssignmentManager).writeTaskContainerMapping("task-3", "test-container-id", TaskMode.Active);
    Mockito.verify(mockTaskAssignmentManager).writeTaskContainerMapping("task-4", "test-container-id", TaskMode.Active);

    // Verify that the old, stale partition mappings had been purged in the coordinator stream.
    Mockito.verify(mockTaskPartitionAssignmentManager).delete(systemStreamPartitions);

    // Verify that the new task to partition assignment is stored in the coordinator stream.
    Mockito.verify(mockTaskPartitionAssignmentManager).writeTaskPartitionAssignment(testSystemStreamPartition1, ImmutableList.of("task-1"));
    Mockito.verify(mockTaskPartitionAssignmentManager).writeTaskPartitionAssignment(testSystemStreamPartition2, ImmutableList.of("task-2"));
    Mockito.verify(mockTaskPartitionAssignmentManager).writeTaskPartitionAssignment(testSystemStreamPartition3, ImmutableList.of("task-3"));
    Mockito.verify(mockTaskPartitionAssignmentManager).writeTaskPartitionAssignment(testSystemStreamPartition4, ImmutableList.of("task-4"));
  }
}
