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

package org.apache.samza.container.grouper.stream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

/**
 * An implementation of {@link SystemStreamPartitionGrouper} that groups the input stream partitions according to their partition number.
 * This leads to a single task processing all the messages of a single partition (e.g. partition 0) across all the input streams.
 * Using this strategy, if two input streams have a partition 0, then all the messages from both partitions will be routed to a single task.
 * This partitioning strategy is useful for joining and aggregating input streams.
 */
public class GroupByPartition implements SystemStreamPartitionGrouper {
  private final Set<SystemStreamPartition> broadcastStreams;
  private final int elasticityFactor;

  public GroupByPartition(Config config) {
    TaskConfig taskConfig = new TaskConfig(config);
    this.broadcastStreams = taskConfig.getBroadcastSystemStreamPartitions();
    elasticityFactor = new JobConfig(config).getElasticityFactor();
  }

  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> ssps) {
    Map<TaskName, Set<SystemStreamPartition>> groupedMap = new HashMap<>();

    for (SystemStreamPartition ssp : ssps) {
      // Broadcast streams are part of all the tasks. They are added to each task at the end.
      if (broadcastStreams.contains(ssp)) {
        continue;
      }

      // if elasticity factor > 1 then elasticity is enabled
      // for each partition create ElasticityFactor number of tasks
      // i.e; result will have number of tasks =  ElasticityFactor X number of partitions
      // each task will have name Partition_X_Y where X is the partition number and Y <= elasticityFactor
      // each task Partition_X_Y consumes the keyBucket Y of all the SSP with partition number X.
      for (int i = 0; i < elasticityFactor; i++) {
        int keyBucket = elasticityFactor == 1 ? -1 : i;
        String taskNameStr = elasticityFactor == 1 ?
            String.format("Partition %d", ssp.getPartition().getPartitionId()) :
            String.format("Partition %d_%d_%d", ssp.getPartition().getPartitionId(), keyBucket, elasticityFactor);
        TaskName taskName = new TaskName(taskNameStr);
        SystemStreamPartition sspWithKeyBucket = new SystemStreamPartition(ssp, keyBucket);
        groupedMap.putIfAbsent(taskName, new HashSet<>());
        groupedMap.get(taskName).add(sspWithKeyBucket);
      }
    }

    // assign the broadcast streams to all the taskNames
    if (!broadcastStreams.isEmpty()) {
      for (Set<SystemStreamPartition> value : groupedMap.values()) {
        value.addAll(broadcastStreams);
      }
    }

    return groupedMap;
  }
}