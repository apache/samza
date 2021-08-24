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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.GrouperMetadata;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.container.grouper.task.TaskPartitionAssignmentManager;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestJobModelHelper {
  private static final SystemStream SYSTEM_STREAM0 = new SystemStream("system0", "stream0");
  private static final SystemStream SYSTEM_STREAM1 = new SystemStream("system1", "stream1");
  private static final int NUM_CONTAINERS = 2;
  private static final String HOST0 = "host-0.abc";
  private static final String HOST1 = "host-1.abc";

  @Mock
  private LocalityManager localityManager;
  @Mock
  private LocalityModel localityModel;
  @Mock
  private TaskAssignmentManager taskAssignmentManager;
  @Mock
  private TaskPartitionAssignmentManager taskPartitionAssignmentManager;
  @Mock
  private StreamMetadataCache streamMetadataCache;
  @Mock
  private JobModelCalculator jobModelCalculator;
  @Mock
  private Map<TaskName, Integer> changelogPartitionMapping;
  @Captor
  private ArgumentCaptor<GrouperMetadata> grouperMetadataArgumentCaptor;

  private JobModelHelper jobModelHelper;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(this.localityManager.readLocality()).thenReturn(this.localityModel);
    this.jobModelHelper =
        new JobModelHelper(this.localityManager, this.taskAssignmentManager, this.taskPartitionAssignmentManager,
            this.streamMetadataCache, this.jobModelCalculator);
  }

  @Test
  public void testNoPreviousJob() {
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0), taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(3), taskModel(3, 3))));
    runAndCheckNewJobModel(config(), containerModels);
    verifyGrouperMetadata(anyHostProcessorIdToLocationId(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());
    verifyNewTaskAssignments(ImmutableSet.of(), allSSPs(containerModels), containerToTaskToMode(containerModels),
        sspToTasks(containerModels));
  }

  @Test
  public void testSameJobModelAsPrevious() {
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0), taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(3), taskModel(3, 3))));
    setupOldTaskAssignments(containerModels);
    runAndCheckNewJobModel(config(), containerModels);
    verifyGrouperMetadata(anyHostProcessorIdToLocationId(), anyHostTaskNameToLocationId(4),
        activeTaskToSSPs(containerModels), activeTaskToContainer(containerModels));
    verifyNewTaskAssignments(null, null, containerToTaskToMode(containerModels), sspToTasks(containerModels));
  }

  @Test
  public void testNewContainerWithProcessorLocality() {
    Map<String, ContainerModel> oldContainerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0))));
    Map<String, ProcessorLocality> processorLocalities = ImmutableMap.of("0", processorLocality("0", HOST0));
    when(this.localityModel.getProcessorLocalities()).thenReturn(processorLocalities);
    setupOldTaskAssignments(oldContainerModels);
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0), taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(3), taskModel(3, 3))));
    runAndCheckNewJobModel(config(), containerModels);
    verifyGrouperMetadata(processorIdToLocationId(ImmutableMap.of("0", HOST0)),
        ImmutableMap.of(taskName(0), new LocationId(HOST0)), activeTaskToSSPs(oldContainerModels),
        activeTaskToContainer(oldContainerModels));
    verifyNewTaskAssignments(taskNameStrings(oldContainerModels, TaskMode.Active), allSSPs(containerModels),
        containerToTaskToMode(containerModels), sspToTasks(containerModels));
  }

  @Test
  public void testAllProcessorLocalityExists() {
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0), taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(3), taskModel(3, 3))));
    Map<String, ProcessorLocality> processorLocalities =
        ImmutableMap.of("0", processorLocality("0", HOST0), "1", processorLocality("1", HOST1));
    when(this.localityModel.getProcessorLocalities()).thenReturn(processorLocalities);
    setupOldTaskAssignments(containerModels);
    runAndCheckNewJobModel(config(), containerModels);
    verifyGrouperMetadata(processorIdToLocationId(ImmutableMap.of("0", HOST0, "1", HOST1)),
        ImmutableMap.of(taskName(0), new LocationId(HOST0), taskName(1), new LocationId(HOST1), taskName(2),
            new LocationId(HOST0), taskName(3), new LocationId(HOST1)), activeTaskToSSPs(containerModels),
        activeTaskToContainer(containerModels));
    verifyNewTaskAssignments(null, null, containerToTaskToMode(containerModels), sspToTasks(containerModels));
  }

  @Test
  public void testAddBroadcastInput() {
    Map<String, ContainerModel> oldContainerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0))), "1",
            new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1))));
    setupOldTaskAssignments(oldContainerModels);
    TaskModel taskModel0 = taskModel(0, ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(0)),
        new SystemStreamPartition(SYSTEM_STREAM1, new Partition(0))));
    TaskModel taskModel1 = taskModel(1, ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(1)),
        new SystemStreamPartition(SYSTEM_STREAM1, new Partition(0))));
    Map<String, ContainerModel> containerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel0)), "1",
            new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel1)));
    runAndCheckNewJobModel(config(), containerModels);
    verifyGrouperMetadata(anyHostProcessorIdToLocationId(), anyHostTaskNameToLocationId(2),
        activeTaskToSSPs(oldContainerModels), activeTaskToContainer(oldContainerModels));
    verifyNewTaskAssignments(null, null, containerToTaskToMode(containerModels), sspToTasks(containerModels));
  }

  @Test
  public void testExistingBroadcastInput() {
    TaskModel taskModel0 = taskModel(0, ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(0)),
        new SystemStreamPartition(SYSTEM_STREAM1, new Partition(0))));
    TaskModel taskModel1 = taskModel(1, ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(1)),
        new SystemStreamPartition(SYSTEM_STREAM1, new Partition(0))));
    Map<String, ContainerModel> containerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel0)), "1",
            new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel1)));
    setupOldTaskAssignments(containerModels);
    runAndCheckNewJobModel(config(), containerModels);
    verifyGrouperMetadata(anyHostProcessorIdToLocationId(), anyHostTaskNameToLocationId(2),
        activeTaskToSSPs(containerModels), activeTaskToContainer(containerModels));
    verifyNewTaskAssignments(null, null, containerToTaskToMode(containerModels), sspToTasks(containerModels));
  }

  @Test
  public void testNewStandbyContainers() {
    Map<String, ContainerModel> oldContainerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0))), "1",
            new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1))));
    setupOldTaskAssignments(oldContainerModels);
    Map<String, ContainerModel> containerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0))), "1",
            new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1))), "0-0",
            new ContainerModel("0-0", ImmutableMap.of(standbyTaskName(0, 0), standbyTaskModel(0, 0))), "1-0",
            new ContainerModel("1-0", ImmutableMap.of(standbyTaskName(1, 0), standbyTaskModel(1, 0))));
    runAndCheckNewJobModel(config(), containerModels);
    verifyGrouperMetadata(anyHostProcessorIdToLocationId(), anyHostTaskNameToLocationId(2),
        activeTaskToSSPs(oldContainerModels), activeTaskToContainer(oldContainerModels));
    // TaskAssignmentManager.deleteTaskContainerMappings is called once due to change in standby task count
    verifyNewTaskAssignments(ImmutableSet.of(), null, containerToTaskToMode(containerModels),
        sspToTasks(containerModels));
  }

  @Test
  public void testExistingStandbyContainers() {
    Map<String, ContainerModel> oldContainerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0))), "1",
            new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1))), "0-0",
            new ContainerModel("0-0", ImmutableMap.of(standbyTaskName(0, 0), standbyTaskModel(0, 0))), "1-0",
            new ContainerModel("1-0", ImmutableMap.of(standbyTaskName(1, 0), standbyTaskModel(1, 0))));
    setupOldTaskAssignments(oldContainerModels);
    Map<String, ContainerModel> containerModels = new ImmutableMap.Builder<String, ContainerModel>().put("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0))))
        .put("1", new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1))))
        .put("0-0", new ContainerModel("0-0", ImmutableMap.of(standbyTaskName(0, 0), standbyTaskModel(0, 0))))
        .put("1-0", new ContainerModel("1-0", ImmutableMap.of(standbyTaskName(1, 0), standbyTaskModel(1, 0))))
        .put("0-1", new ContainerModel("0-1", ImmutableMap.of(standbyTaskName(0, 1), standbyTaskModel(0, 1))))
        .put("1-1", new ContainerModel("1-1", ImmutableMap.of(standbyTaskName(1, 1), standbyTaskModel(1, 1))))
        .build();
    runAndCheckNewJobModel(config(), containerModels);
    verifyGrouperMetadata(anyHostProcessorIdToLocationId(), anyHostTaskNameToLocationId(2),
        activeTaskToSSPs(oldContainerModels), activeTaskToContainer(oldContainerModels));
    verifyNewTaskAssignments(taskNameStrings(oldContainerModels, TaskMode.Standby), null,
        containerToTaskToMode(containerModels), sspToTasks(containerModels));
  }

  @Test
  public void testNewStandbyTasks() {
    Map<String, ContainerModel> oldContainerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0))), "1",
            new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1))), "0-0",
            new ContainerModel("0-0", ImmutableMap.of(standbyTaskName(0, 0), standbyTaskModel(0, 0))), "1-0",
            new ContainerModel("1-0", ImmutableMap.of(standbyTaskName(1, 0), standbyTaskModel(1, 0))));
    setupOldTaskAssignments(oldContainerModels);
    Map<String, ContainerModel> containerModels = new ImmutableMap.Builder<String, ContainerModel>().put("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0))))
        .put("1", new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1))))
        .put("2", new ContainerModel("2", ImmutableMap.of(taskName(2), taskModel(2, 2))))
        .put("0-0", new ContainerModel("0-0", ImmutableMap.of(standbyTaskName(0, 0), standbyTaskModel(0, 0))))
        .put("1-0", new ContainerModel("1-0", ImmutableMap.of(standbyTaskName(1, 0), standbyTaskModel(1, 0))))
        .put("2-0", new ContainerModel("2-0", ImmutableMap.of(standbyTaskName(2, 0), standbyTaskModel(2, 0))))
        .build();
    runAndCheckNewJobModel(config(), containerModels);
    verifyGrouperMetadata(anyHostProcessorIdToLocationId(), anyHostTaskNameToLocationId(2),
        activeTaskToSSPs(oldContainerModels), activeTaskToContainer(oldContainerModels));
    //noinspection unchecked: ArgumentCaptor doesn't ideally handle multiple levels of generics, so need cast
    ArgumentCaptor<Iterable<String>> deleteTaskContainerMappingsCaptor =
        (ArgumentCaptor<Iterable<String>>) (ArgumentCaptor<?>) ArgumentCaptor.forClass(Iterable.class);
    verify(this.taskAssignmentManager, times(2)).deleteTaskContainerMappings(
        deleteTaskContainerMappingsCaptor.capture());
    assertEquals(taskNameStrings(oldContainerModels, TaskMode.Active),
        ImmutableSet.copyOf(deleteTaskContainerMappingsCaptor.getAllValues().get(0)));
    assertEquals(taskNameStrings(oldContainerModels, TaskMode.Standby),
        ImmutableSet.copyOf(deleteTaskContainerMappingsCaptor.getAllValues().get(1)));
    verify(this.taskPartitionAssignmentManager).delete(allSSPs(containerModels));
    verify(this.taskPartitionAssignmentManager).delete(any());
    verify(this.taskAssignmentManager).writeTaskContainerMappings(containerToTaskToMode(containerModels));
    verify(this.taskAssignmentManager).writeTaskContainerMappings(any());
    verify(this.taskPartitionAssignmentManager).writeTaskPartitionAssignments(sspToTasks(containerModels));
    verify(this.taskPartitionAssignmentManager).writeTaskPartitionAssignments(any());
  }

  private void runAndCheckNewJobModel(Config config, Map<String, ContainerModel> containerModels) {
    JobModel jobModel = new JobModel(config, containerModels);
    when(this.jobModelCalculator.calculateJobModel(eq(config), eq(this.changelogPartitionMapping),
        eq(this.streamMetadataCache), this.grouperMetadataArgumentCaptor.capture())).thenReturn(jobModel);
    JobModel actualJobModel = this.jobModelHelper.newJobModel(config, this.changelogPartitionMapping);
    assertEquals(jobModel, actualJobModel);
  }

  private void setupOldTaskAssignments(Map<String, ContainerModel> oldContainerModels) {
    when(this.taskAssignmentManager.readTaskAssignment()).thenReturn(taskNameStringToContainer(oldContainerModels));
    when(this.taskAssignmentManager.readTaskModes()).thenReturn(taskNameToMode(oldContainerModels));
    when(this.taskPartitionAssignmentManager.readTaskPartitionAssignments()).thenReturn(sspToTasks(oldContainerModels));
  }

  private static Config config() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_CONTAINER_COUNT, Integer.toString(NUM_CONTAINERS));
    return new MapConfig(configMap);
  }

  private static TaskName taskName(int id) {
    return new TaskName("Partition " + id);
  }

  private static TaskModel taskModel(int id, int partitionForSystemStream0) {
    return taskModel(id,
        ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(partitionForSystemStream0))));
  }

  private static TaskModel taskModel(int id, int partitionForSystemStream0, int partitionForSystemStream1) {
    return taskModel(id,
        ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(partitionForSystemStream0)),
            new SystemStreamPartition(SYSTEM_STREAM1, new Partition(partitionForSystemStream1))));
  }

  private static TaskModel taskModel(int id, Set<SystemStreamPartition> systemStreamPartitions) {
    return new TaskModel(taskName(id), systemStreamPartitions, new Partition(id));
  }

  private static TaskName standbyTaskName(int taskId, int standbyId) {
    return new TaskName(String.format("Standby-Partition %s-%s", taskId, standbyId));
  }

  private static TaskModel standbyTaskModel(int taskId, int standbyId) {
    TaskName taskName = new TaskName(String.format("Standby-Partition %s-%s", taskId, standbyId));
    Set<SystemStreamPartition> ssps = ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(taskId)));
    return new TaskModel(taskName, ssps, new Partition(taskId), TaskMode.Standby);
  }

  private static Set<SystemStreamPartition> allSSPs(Map<String, ContainerModel> containerModels) {
    return taskModelStream(containerModels).flatMap(taskModel -> taskModel.getSystemStreamPartitions().stream())
        .collect(Collectors.toSet());
  }

  private static Map<String, Map<String, TaskMode>> containerToTaskToMode(Map<String, ContainerModel> containerModels) {
    return containerModels.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()
            .getTasks()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(taskModelEntry -> taskModelEntry.getKey().getTaskName(),
              taskModelEntry -> taskModelEntry.getValue().getTaskMode()))));
  }

  private static Stream<TaskModel> taskModelStream(Map<String, ContainerModel> containerModels) {
    return containerModels.values().stream().flatMap(containerModel -> containerModel.getTasks().values().stream());
  }

  private static Set<TaskName> taskNames(Map<String, ContainerModel> containerModels, TaskMode taskMode) {
    return taskModelStream(containerModels).filter(taskModel -> taskMode.equals(taskModel.getTaskMode()))
        .map(TaskModel::getTaskName)
        .collect(Collectors.toSet());
  }

  private static Set<String> taskNameStrings(Map<String, ContainerModel> containerModels, TaskMode taskMode) {
    return taskNames(containerModels, taskMode).stream().map(TaskName::getTaskName).collect(Collectors.toSet());
  }

  private static Map<TaskName, List<SystemStreamPartition>> activeTaskToSSPs(
      Map<String, ContainerModel> containerModels) {
    return taskModelStream(containerModels).filter(taskModel -> TaskMode.Active.equals(taskModel.getTaskMode()))
        .collect(Collectors.toMap(TaskModel::getTaskName,
          taskModel -> ImmutableList.copyOf(taskModel.getSystemStreamPartitions())));
  }

  private static Map<TaskName, TaskMode> taskNameToMode(Map<String, ContainerModel> containerModels) {
    return taskModelStream(containerModels).collect(Collectors.toMap(TaskModel::getTaskName, TaskModel::getTaskMode));
  }

  private static Map<TaskName, String> activeTaskToContainer(Map<String, ContainerModel> containerModels) {
    Map<TaskName, String> taskToContainerMap = new HashMap<>();
    containerModels.values()
        .forEach(containerModel -> containerModel.getTasks()
            .values()
            .stream()
            .filter(taskModel -> TaskMode.Active.equals(taskModel.getTaskMode()))
            .forEach(taskModel -> taskToContainerMap.put(taskModel.getTaskName(), containerModel.getId())));
    return taskToContainerMap;
  }

  private static Map<String, String> taskNameStringToContainer(Map<String, ContainerModel> containerModels) {
    return activeTaskToContainer(containerModels).entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> entry.getKey().getTaskName(), Map.Entry::getValue));
  }

  private static Map<SystemStreamPartition, List<String>> sspToTasks(Map<String, ContainerModel> containerModels) {
    Map<SystemStreamPartition, List<String>> sspToTasksMap = new HashMap<>();
    taskModelStream(containerModels).forEach(taskModel -> taskModel.getSystemStreamPartitions().forEach(ssp -> {
      sspToTasksMap.putIfAbsent(ssp, new ArrayList<>());
      sspToTasksMap.get(ssp).add(taskModel.getTaskName().getTaskName());
    }));
    return sspToTasksMap;
  }

  private static Map<String, LocationId> processorIdToLocationId(Map<String, String> processorIdToHostOverrides) {
    Map<String, LocationId> processorLocality = new HashMap<>(anyHostProcessorIdToLocationId());
    processorIdToHostOverrides.forEach((processorId, host) -> processorLocality.put(processorId, new LocationId(host)));
    return processorLocality;
  }

  private static Map<String, LocationId> anyHostProcessorIdToLocationId() {
    return IntStream.range(0, NUM_CONTAINERS)
        .boxed()
        .collect(Collectors.toMap(i -> Integer.toString(i), i -> new LocationId("ANY_HOST")));
  }

  private static Map<TaskName, LocationId> anyHostTaskNameToLocationId(int numTasks) {
    return IntStream.range(0, numTasks)
        .boxed()
        .collect(Collectors.toMap(TestJobModelHelper::taskName, i -> new LocationId("ANY_HOST")));
  }

  private void verifyGrouperMetadata(Map<String, LocationId> processorLocality, Map<TaskName, LocationId> taskLocality,
      Map<TaskName, List<SystemStreamPartition>> previousTaskToSSPAssignment,
      Map<TaskName, String> previousTaskToProcessorAssignment) {
    GrouperMetadata grouperMetadata = this.grouperMetadataArgumentCaptor.getValue();
    assertEquals(processorLocality, grouperMetadata.getProcessorLocality());
    assertEquals(taskLocality, grouperMetadata.getTaskLocality());
    assertPreviousTaskToSSPAssignment(previousTaskToSSPAssignment, grouperMetadata.getPreviousTaskToSSPAssignment());
    assertEquals(previousTaskToProcessorAssignment, grouperMetadata.getPreviousTaskToProcessorAssignment());
  }

  private void verifyNewTaskAssignments(Set<String> deleteTaskContainerMappings,
      Set<SystemStreamPartition> taskPartitionAssignmentDeletes,
      Map<String, Map<String, TaskMode>> writeTaskContainerMappings,
      Map<SystemStreamPartition, List<String>> writeTaskPartitionAssignments) {
    if (deleteTaskContainerMappings != null) {
      //noinspection unchecked: ArgumentCaptor doesn't ideally handle multiple levels of generics, so need cast
      ArgumentCaptor<Iterable<String>> deleteTaskContainerMappingsCaptor =
          (ArgumentCaptor<Iterable<String>>) (ArgumentCaptor<?>) ArgumentCaptor.forClass(Iterable.class);
      verify(this.taskAssignmentManager).deleteTaskContainerMappings(deleteTaskContainerMappingsCaptor.capture());
      // order of Iterable argument shouldn't matter
      assertEquals(deleteTaskContainerMappings, ImmutableSet.copyOf(deleteTaskContainerMappingsCaptor.getValue()));
    } else {
      verify(this.taskAssignmentManager, never()).deleteTaskContainerMappings(any());
    }
    if (taskPartitionAssignmentDeletes != null) {
      verify(this.taskPartitionAssignmentManager).delete(taskPartitionAssignmentDeletes);
      verify(this.taskPartitionAssignmentManager).delete(any());
    } else {
      verify(this.taskPartitionAssignmentManager, never()).delete(any());
    }

    verify(this.taskAssignmentManager).writeTaskContainerMappings(writeTaskContainerMappings);
    // verifies no calls to writeTaskContainerMappings other than the one above
    verify(this.taskAssignmentManager).writeTaskContainerMappings(any());

    verify(this.taskPartitionAssignmentManager).writeTaskPartitionAssignments(writeTaskPartitionAssignments);
    // verifies no calls to writeTaskPartitionAssignments other than the one above
    verify(this.taskPartitionAssignmentManager).writeTaskPartitionAssignments(any());
  }

  /**
   * Order of SSPs in values of the map shouldn't matter, and the order is also non-deterministic, so just check the
   * contents of the lists.
   */
  private static void assertPreviousTaskToSSPAssignment(Map<TaskName, List<SystemStreamPartition>> expected,
      Map<TaskName, List<SystemStreamPartition>> actual) {
    assertEquals(expected.size(), actual.size());
    expected.forEach((taskName, sspList) -> assertContentsEqualNoDuplicates(sspList, actual.get(taskName)));
  }

  private static <T> void assertContentsEqualNoDuplicates(List<T> expected, List<T> actual) {
    Set<T> expectedSet = ImmutableSet.copyOf(expected);
    Set<T> actualSet = ImmutableSet.copyOf(actual);
    assertEquals(expectedSet.size(), expected.size());
    assertEquals(actualSet.size(), actual.size());
    assertEquals(expectedSet, actualSet);
  }

  private static ProcessorLocality processorLocality(String id, String host) {
    ProcessorLocality processorLocality = mock(ProcessorLocality.class);
    when(processorLocality.id()).thenReturn(id);
    when(processorLocality.host()).thenReturn(host);
    return processorLocality;
  }
}