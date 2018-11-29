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
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

/**
 * An implementation of {@link SystemStreamPartitionGrouper} that groups the input stream partitions according to their partition number.
 *
 * This leads to a single task processing all the messages of a single partition(e.g. partition 0) across all the input streams.
 *
 * Using this strategy, if two input streams have a partition 0, then all the messages from both partitions will be routed to a single task.
 *
 * This partitioning strategy is useful for joining and aggregating input streams.
 */
public class GroupByPartition implements SystemStreamPartitionGrouper {
  private final Set<SystemStreamPartition> broadcastStreams;

  public GroupByPartition(Config config) {
    TaskConfigJava taskConfig = new TaskConfigJava(config);
    this.broadcastStreams = taskConfig.getBroadcastSystemStreamPartitions();
  }

  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> ssps) {
    Map<TaskName, Set<SystemStreamPartition>> groupedMap = new HashMap<>();

    for (SystemStreamPartition ssp : ssps) {
      // Broadcast streams are part of all the tasks. They are added to each task at the end.
      if (broadcastStreams.contains(ssp)) {
        continue;
      }

      TaskName taskName = new TaskName(String.format("Partition %d", ssp.getPartition().getPartitionId()));

      if (!groupedMap.containsKey(taskName)) {
        groupedMap.put(taskName, new HashSet<>());
      }
      groupedMap.get(taskName).add(ssp);
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