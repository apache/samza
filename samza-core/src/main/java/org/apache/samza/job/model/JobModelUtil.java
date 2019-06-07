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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStream;
import org.apache.samza.SamzaException;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Utility class for the {@link JobModel}
 */
public class JobModelUtil {

  private static final ObjectMapper MAPPER = SamzaObjectMapper.getObjectMapper();

  private static final int JOB_MODEL_SEGMENT_SIZE_IN_BYTES = 1020 * 1020;

  private static final String UTF_8 = "UTF-8";

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
   * Splits the {@link JobModel} into independent byte array segments of 1 MB size.
   * @param jobModel the job model to split.
   * @return the job model splitted into independent byte array chunks.
   */
  private static List<byte[]> chunkJobModel(JobModel jobModel) {
    try {
      String jobModelSerializedAsString = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jobModel);
      List<byte[]> jobModelSegments = new ArrayList<>();
      for (int index = 0; index < jobModelSerializedAsString.length(); index += JOB_MODEL_SEGMENT_SIZE_IN_BYTES) {
        String jobModelSegment = jobModelSerializedAsString.substring(index, Math.min(index + JOB_MODEL_SEGMENT_SIZE_IN_BYTES, jobModelSerializedAsString.length()));
        jobModelSegments.add(jobModelSegment.getBytes(Charset.forName(UTF_8)));
      }
      return jobModelSegments;
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception occurred when splitting the JobModel: %s to small chunks.", jobModel), e);
    }
  }

  /**
   * Splits the larger {@link JobModel} into independent segments of 1 MB size and stores them in {@link MetadataStore}.
   * @param jobModel the job model to store into {@link MetadataStore}.
   * @param jobModelVersion the job model version.
   * @param metadataStore the metadata store.
   */
  public static void writeJobModel(JobModel jobModel, String jobModelVersion, MetadataStore metadataStore) {
    try {
      List<byte[]> jobModelSegments = chunkJobModel(jobModel);
      for (int jobModelSegmentIndex = 0; jobModelSegmentIndex < jobModelSegments.size(); jobModelSegmentIndex += 1) {
        String jobModelSegmentKey = getJobModelSegmentKey(jobModelVersion, jobModelSegmentIndex);
        metadataStore.put(jobModelSegmentKey, jobModelSegments.get(jobModelSegmentIndex));
      }
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
      StringBuilder jobModelUnificationBuffer = new StringBuilder();
      for (int jobModelSegmentIndex = 0;; ++jobModelSegmentIndex) {
        String jobModelSegmentKey = getJobModelSegmentKey(jobModelVersion, jobModelSegmentIndex);
        byte[] jobModelSegment = metadataStore.get(jobModelSegmentKey);
        if (jobModelSegment == null) {
          break;
        } else {
          jobModelUnificationBuffer.append(new String(jobModelSegment, Charset.forName(UTF_8)));
        }
      }
      return MAPPER.readValue(jobModelUnificationBuffer.toString(), JobModel.class);
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception occurred when reading JobModel version: %s from metadata store.", jobModelVersion), e);
    }
  }

  private static String getJobModelSegmentKey(String jobModelVersion, int jobModelSegmentIndex) {
    return String.format("%s/%d", jobModelVersion, jobModelSegmentIndex);
  }

  public static Set<SystemStream> getSystemStreams(JobModel jobModel) {
    Map<TaskName, Set<SystemStreamPartition>> taskToSSPs = getTaskToSystemStreamPartitions(jobModel);
    return taskToSSPs.values().stream().flatMap(taskSSPs -> taskSSPs.stream().map(ssp -> ssp.getSystemStream())).collect(Collectors.toSet());
  }

  private JobModelUtil() { }
}
