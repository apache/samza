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
package org.apache.samza.job.model;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class for the {@link JobModel}
 */
public class JobModelUtil {

  private static final ObjectMapper MAPPER = SamzaObjectMapper.getObjectMapper();

  private static final String UTF_8 = "UTF-8";

  private static final String JOB_MODEL_GENERATION_KEY = "jobModelGeneration/jobModels";

  /**
   * A helper method to fetch the task names associated with the processor from the job model.
   * @param processorId processor for which task names are fetched
   * @param jobModel job model
   * @return a set of {@code TaskName} associated with the processor from the job model.
   */
  public static Set<TaskName> getTaskNamesForProcessor(String processorId, JobModel jobModel) {
    Preconditions.checkNotNull(jobModel, "JobModel cannot be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(processorId), "ProcessorId cannot be empty or null");

    return Optional.ofNullable(jobModel.getContainers().get(processorId))
        .map(ContainerModel::getTasks)
        .map(Map::keySet)
        .orElse(Collections.emptySet());
  }

  /**
   * Extracts the map of {@link SystemStreamPartition}s to {@link TaskName} from the {@link JobModel}
   *
   * @return the extracted map
   */
  public static Map<TaskName, Set<SystemStreamPartition>> getTaskToSystemStreamPartitions(JobModel jobModel) {
    Preconditions.checkArgument(jobModel != null, "JobModel cannot be null");

    Map<String, ContainerModel> containers = jobModel.getContainers();
    HashMap<TaskName, Set<SystemStreamPartition>> taskToSSPs = new HashMap<>();
    for (ContainerModel containerModel : containers.values()) {
      for (TaskName taskName : containerModel.getTasks().keySet()) {
        TaskModel taskModel = containerModel.getTasks().get(taskName);
        if (taskModel.getTaskMode() != TaskMode.Active) {
          // Avoid duplicate tasks
          continue;
        }
        taskToSSPs.putIfAbsent(taskName, new HashSet<>());
        taskToSSPs.get(taskName).addAll(taskModel.getSystemStreamPartitions());
      }
    }
    return taskToSSPs;
  }


  /**
   * Converts the JobModel into a byte array into {@link MetadataStore}.
   * @param jobModel the job model to store into {@link MetadataStore}.
   * @param jobModelVersion the job model version.
   * @param metadataStore the metadata store.
   */
  public static void writeJobModel(JobModel jobModel, String jobModelVersion, MetadataStore metadataStore) {
    try {
      String jobModelSerializedAsString = MAPPER.writeValueAsString(jobModel);
      byte[] jobModelSerializedAsBytes = jobModelSerializedAsString.getBytes(UTF_8);
      String metadataStoreKey = getJobModelKey(jobModelVersion);
      metadataStore.put(metadataStoreKey, jobModelSerializedAsBytes);
      metadataStore.flush();
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception occurred when storing JobModel: %s with version: %s.", jobModel, jobModelVersion), e);
    }
  }

  /**
   * Reads and returns the {@link JobModel} from {@link MetadataStore}.
   * @param metadataStore the metadata store.
   * @return the job model read from the metadata store.
   */
  public static JobModel readJobModel(String jobModelVersion, MetadataStore metadataStore) {
    try {
      byte[] jobModelAsBytes = metadataStore.get(getJobModelKey(jobModelVersion));
      return MAPPER.readValue(new String(jobModelAsBytes, UTF_8), JobModel.class);
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception occurred when reading JobModel version: %s from metadata store.", jobModelVersion), e);
    }
  }

  private static String getJobModelKey(String version) {
    return String.format("%s/%s", JOB_MODEL_GENERATION_KEY, version);
  }

  public static Set<SystemStream> getSystemStreams(JobModel jobModel) {
    Map<TaskName, Set<SystemStreamPartition>> taskToSSPs = getTaskToSystemStreamPartitions(jobModel);
    return taskToSSPs.values().stream().flatMap(taskSSPs -> taskSSPs.stream().map(ssp -> ssp.getSystemStream())).collect(Collectors.toSet());
  }

  private JobModelUtil() { }
}
