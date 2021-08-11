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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.GrouperMetadata;
import org.apache.samza.container.grouper.task.GrouperMetadataImpl;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.container.grouper.task.TaskPartitionAssignmentManager;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobModelHelper {
  private static final Logger LOG = LoggerFactory.getLogger(JobModelHelper.class);

  private final LocalityManager localityManager;
  private final TaskAssignmentManager taskAssignmentManager;
  private final TaskPartitionAssignmentManager taskPartitionAssignmentManager;
  private final StreamMetadataCache streamMetadataCache;
  private final JobModelCalculator jobModelCalculator;

  public JobModelHelper(LocalityManager localityManager, TaskAssignmentManager taskAssignmentManager,
      TaskPartitionAssignmentManager taskPartitionAssignmentManager, StreamMetadataCache streamMetadataCache,
      JobModelCalculator jobModelCalculator) {
    this.localityManager = localityManager;
    this.taskAssignmentManager = taskAssignmentManager;
    this.taskPartitionAssignmentManager = taskPartitionAssignmentManager;
    this.streamMetadataCache = streamMetadataCache;
    this.jobModelCalculator = jobModelCalculator;
  }

  public JobModel newJobModel(Config config, Map<TaskName, Integer> changelogPartitionMapping) {
    GrouperMetadata grouperMetadata = getGrouperMetadata(config, this.localityManager, this.taskAssignmentManager,
        this.taskPartitionAssignmentManager);
    JobModel jobModel =
        this.jobModelCalculator.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            grouperMetadata);
    updateTaskAssignments(jobModel, this.taskAssignmentManager, this.taskPartitionAssignmentManager, grouperMetadata);
    return jobModel;
  }

  private GrouperMetadata getGrouperMetadata(Config config, LocalityManager localityManager,
      TaskAssignmentManager taskAssignmentManager, TaskPartitionAssignmentManager taskPartitionAssignmentManager) {
    Map<String, LocationId> processorLocality = getProcessorLocality(config, localityManager);
    Map<TaskName, TaskMode> taskModes = taskAssignmentManager.readTaskModes();

    Map<TaskName, String> taskNameToProcessorId = new HashMap<>();
    Map<TaskName, LocationId> taskLocality = new HashMap<>();
    // We read the taskAssignment only for ActiveTasks, i.e., tasks that have no task-mode or have an active task mode
    taskAssignmentManager.readTaskAssignment().forEach((taskNameString, containerId) -> {
      TaskName taskName = new TaskName(taskNameString);
      if (isActiveTask(taskName, taskModes)) {
        taskNameToProcessorId.put(taskName, containerId);
        if (processorLocality.containsKey(containerId)) {
          taskLocality.put(taskName, processorLocality.get(containerId));
        }
      }
    });

    Map<SystemStreamPartition, List<String>> sspToTaskMapping =
        taskPartitionAssignmentManager.readTaskPartitionAssignments();
    Map<TaskName, List<SystemStreamPartition>> taskPartitionAssignments = new HashMap<>();
    // Task to partition assignments is stored as {@see SystemStreamPartition} to list of {@see TaskName} in
    // coordinator stream. This is done due to the 1 MB value size limit in a kafka topic. Conversion to
    // taskName to SystemStreamPartitions is done here to wire-in the data to {@see JobModel}.
    sspToTaskMapping.forEach((systemStreamPartition, taskNames) -> taskNames.forEach(taskNameString -> {
      TaskName taskName = new TaskName(taskNameString);
      if (isActiveTask(taskName, taskModes)) {
        taskPartitionAssignments.putIfAbsent(taskName, new ArrayList<>());
        taskPartitionAssignments.get(taskName).add(systemStreamPartition);
      }
    }));
    return new GrouperMetadataImpl(processorLocality, taskLocality, taskPartitionAssignments, taskNameToProcessorId);
  }

  /**
   * Retrieves and returns the processor locality of a samza job using provided {@see Config} and {@see LocalityManager}.
   * @param config provides the configurations defined by the user. Required to connect to the storage layer.
   * @param localityManager provides the processor to host mapping persisted to the metadata store.
   * @return the processor locality.
   */
  private static Map<String, LocationId> getProcessorLocality(Config config, LocalityManager localityManager) {
    Map<String, LocationId> containerToLocationId = new HashMap<>();
    Map<String, ProcessorLocality> existingContainerLocality = localityManager.readLocality().getProcessorLocalities();

    for (int i = 0; i < new JobConfig(config).getContainerCount(); i++) {
      String containerId = Integer.toString(i);
      LocationId locationId = Optional.ofNullable(existingContainerLocality.get(containerId))
          .map(ProcessorLocality::host)
          .filter(StringUtils::isNotEmpty)
          .map(LocationId::new)
          // To handle the case when the container count is increased between two different runs of a samza-yarn job,
          // set the locality of newly added containers to any_host.
          .orElse(new LocationId("ANY_HOST"));
      containerToLocationId.put(containerId, locationId);
    }
    return containerToLocationId;
  }

  /**
   * This method does the following:
   * 1. Deletes the existing task assignments if the partition-task grouping has changed from the previous run of the job.
   * 2. Saves the newly generated task assignments to the storage layer through the {@param TaskAssignementManager}.
   *
   * @param jobModel              represents the {@see JobModel} of the samza job.
   * @param taskAssignmentManager required to persist the processor to task assignments to the metadata store.
   * @param taskPartitionAssignmentManager required to persist the task to partition assignments to the metadata store.
   * @param grouperMetadata       provides the historical metadata of the samza application.
   */
  private void updateTaskAssignments(JobModel jobModel, TaskAssignmentManager taskAssignmentManager,
      TaskPartitionAssignmentManager taskPartitionAssignmentManager, GrouperMetadata grouperMetadata) {
    LOG.info("Storing the task assignments into metadata store.");
    Set<String> activeTaskNames = new HashSet<>();
    Set<String> standbyTaskNames = new HashSet<>();
    Set<SystemStreamPartition> systemStreamPartitions = new HashSet<>();

    for (ContainerModel containerModel : jobModel.getContainers().values()) {
      for (TaskModel taskModel : containerModel.getTasks().values()) {
        if (TaskMode.Active.equals(taskModel.getTaskMode())) {
          activeTaskNames.add(taskModel.getTaskName().getTaskName());
        }
        if (TaskMode.Standby.equals(taskModel.getTaskMode())) {
          standbyTaskNames.add(taskModel.getTaskName().getTaskName());
        }
        systemStreamPartitions.addAll(taskModel.getSystemStreamPartitions());
      }
    }

    Map<TaskName, String> previousTaskToContainerId = grouperMetadata.getPreviousTaskToProcessorAssignment();
    if (activeTaskNames.size() != previousTaskToContainerId.size()) {
      LOG.warn(String.format(
          "Current task count %s does not match saved task count %s. Stateful jobs may observe misalignment of keys!",
          activeTaskNames.size(), previousTaskToContainerId.size()));
      // If the tasks changed, then the partition-task grouping is also likely changed and we can't handle that
      // without a much more complicated mapping. Further, the partition count may have changed, which means
      // input message keys are likely reshuffled w.r.t. partitions, so the local state may not contain necessary
      // data associated with the incoming keys. Warn the user and default to grouper
      // In this scenario the tasks may have been reduced, so we need to delete all the existing messages
      taskAssignmentManager.deleteTaskContainerMappings(
          previousTaskToContainerId.keySet().stream().map(TaskName::getTaskName).collect(Collectors.toList()));
      taskPartitionAssignmentManager.delete(systemStreamPartitions);
    }

    // if the set of standby tasks has changed, e.g., when the replication-factor changed, or the active-tasks-set has
    // changed, we log a warning and delete the existing mapping for these tasks
    Set<String> previousStandbyTasks = taskAssignmentManager.readTaskModes()
        .entrySet()
        .stream()
        .filter(taskNameToTaskModeEntry -> TaskMode.Standby.equals(taskNameToTaskModeEntry.getValue()))
        .map(taskNameToTaskModeEntry -> taskNameToTaskModeEntry.getKey().getTaskName())
        .collect(Collectors.toSet());
    if (!standbyTaskNames.equals(previousStandbyTasks)) {
      LOG.info(
          String.format("The set of standby tasks has changed, current standby tasks %s, previous standby tasks %s",
              standbyTaskNames, previousStandbyTasks));
      taskAssignmentManager.deleteTaskContainerMappings(previousStandbyTasks);
    }

    // Task to partition assignments is stored as {@see SystemStreamPartition} to list of {@see TaskName} in
    // coordinator stream. This is done due to the 1 MB value size limit in a kafka topic.
    Map<SystemStreamPartition, List<String>> sspToTaskNameMap = new HashMap<>();
    Map<String, Map<String, TaskMode>> taskContainerMappings = new HashMap<>();
    for (ContainerModel containerModel : jobModel.getContainers().values()) {
      containerModel.getTasks().forEach((taskName, taskModel) -> {
        taskContainerMappings.putIfAbsent(containerModel.getId(), new HashMap<>());
        taskContainerMappings.get(containerModel.getId()).put(taskName.getTaskName(), taskModel.getTaskMode());
        taskModel.getSystemStreamPartitions().forEach(systemStreamPartition -> {
          sspToTaskNameMap.putIfAbsent(systemStreamPartition, new ArrayList<>());
          sspToTaskNameMap.get(systemStreamPartition).add(taskName.getTaskName());
        });
      });
    }
    taskAssignmentManager.writeTaskContainerMappings(taskContainerMappings);
    taskPartitionAssignmentManager.writeTaskPartitionAssignments(sspToTaskNameMap);
  }

  private static boolean isActiveTask(TaskName taskName, Map<TaskName, TaskMode> taskModes) {
    return !taskModes.containsKey(taskName) || TaskMode.Active.equals(taskModes.get(taskName));
  }
}
