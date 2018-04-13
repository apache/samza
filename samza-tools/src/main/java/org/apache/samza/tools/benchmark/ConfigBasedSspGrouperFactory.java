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

package org.apache.samza.tools.benchmark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;


/**
 * Grouper that assigns only the subset of partitions configured to the task. This can be used only
 * with {@link org.apache.samza.standalone.PassthroughJobCoordinator}.
 */
public class ConfigBasedSspGrouperFactory implements SystemStreamPartitionGrouperFactory {

  /**
   * Comma separated list of partitions that needs to be assigned to this task.
   */
  public static final String CONFIG_STREAM_PARTITIONS = "streams.%s.partitions";
  private static final String CFG_PARTITIONS_DELIMITER = ",";

  @Override
  public SystemStreamPartitionGrouper getSystemStreamPartitionGrouper(Config config) {
    return new ConfigBasedSspGrouper(config);
  }

  private class ConfigBasedSspGrouper implements SystemStreamPartitionGrouper {
    private final Config config;
    private HashMap<String, Set<Integer>> _streamPartitionsMap = new HashMap<>();

    public ConfigBasedSspGrouper(Config config) {
      this.config = config;
    }

    @Override
    public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> ssps) {
      Set<SystemStreamPartition> filteredSsps = new HashSet<>();
      for (SystemStreamPartition ssp : ssps) {
        Set<Integer> partitions = getPartitions(ssp.getSystemStream());
        if (partitions.contains(ssp.getPartition().getPartitionId())) {
          filteredSsps.add(ssp);
        }
      }
      HashMap<TaskName, Set<SystemStreamPartition>> group = new HashMap<>();
      group.put(new TaskName("TestTask"), filteredSsps);
      return group;
    }

    private Set<Integer> getPartitions(SystemStream systemStream) {
      String streamName = systemStream.getStream();

      if (!_streamPartitionsMap.containsKey(streamName)) {
        String partitions = config.get(String.format(CONFIG_STREAM_PARTITIONS, streamName));
        _streamPartitionsMap.put(streamName, Arrays.stream(partitions.split(CFG_PARTITIONS_DELIMITER))
            .map(Integer::parseInt)
            .collect(Collectors.toSet()));
      }
      return _streamPartitionsMap.get(streamName);
    }
  }
}
