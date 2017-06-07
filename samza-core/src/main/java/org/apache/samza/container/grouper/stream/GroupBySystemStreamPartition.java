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

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

public class GroupBySystemStreamPartition implements SystemStreamPartitionGrouper {
  private TaskConfigJava taskConfig = null;
  private Set<SystemStreamPartition> broadcastStreams = new HashSet<SystemStreamPartition>();

  /**
   * A constructor that accepts job config as the parameter
   *
   * @param config job config
   */
  public GroupBySystemStreamPartition(Config config) {
    if (config.containsKey(TaskConfigJava.BROADCAST_INPUT_STREAMS)) {
      taskConfig = new TaskConfigJava(config);
      broadcastStreams = taskConfig.getBroadcastSystemStreamPartitions();
    }
  }

  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> sspSet) {
    return group(new HashMap<>(), sspSet);
  }

  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(Map<SystemStreamPartition, String> previousSSPTaskAssignment,
                                                         Set<SystemStreamPartition> sspSet) {
    Map<String, Set<String>> previousTaskSetByStream = new HashMap<>();
    for (Map.Entry<SystemStreamPartition, String> entry: previousSSPTaskAssignment.entrySet()) {
      SystemStreamPartition ssp = entry.getKey();
      if (!previousTaskSetByStream.containsKey(ssp.getStream()))
        previousTaskSetByStream.put(ssp.getStream(), new HashSet<>());
      previousTaskSetByStream.get(ssp.getStream()).add(entry.getValue());
    }

    Map<TaskName, Set<SystemStreamPartition>> groupedMap = new HashMap<>();

    for (SystemStreamPartition ssp : sspSet) {
      if (broadcastStreams.contains(ssp)) {
        continue;
      }

      int currentPartitionId = ssp.getPartition().getPartitionId();
      TaskName taskName = new TaskName(ssp.toString());
      Set<String> previousTaskSet = previousTaskSetByStream.get(ssp.getStream());

      if (previousTaskSet != null) {
        int previousPartitionId = currentPartitionId % previousTaskSet.size();
        SystemStreamPartition previousSSP = new SystemStreamPartition(ssp.getSystemStream(), new Partition(previousPartitionId));
        taskName = new TaskName(previousSSPTaskAssignment.get(previousSSP));
      }

      groupedMap.put(taskName, new HashSet<>());
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
