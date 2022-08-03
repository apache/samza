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
 * An implementation of {@link SystemStreamPartitionGrouper} that assigns each input {@link SystemStreamPartition} to a separate task.
 * Provides increased parallelism in message processing within a container. Useful in message processing scenarios involving remote I/O.
 */
public class GroupBySystemStreamPartition implements SystemStreamPartitionGrouper {
  private final Set<SystemStreamPartition> broadcastStreams;
  private final int elasticityFactor;

  public GroupBySystemStreamPartition(Config config) {
    TaskConfig taskConfig = new TaskConfig(config);
    broadcastStreams = taskConfig.getBroadcastSystemStreamPartitions();
    elasticityFactor = new JobConfig(config).getElasticityFactor();
  }

  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> ssps) {
    Map<TaskName, Set<SystemStreamPartition>> groupedMap = new HashMap<TaskName, Set<SystemStreamPartition>>();

    for (SystemStreamPartition ssp : ssps) {
      if (broadcastStreams.contains(ssp)) {
        continue;
      }

      // if elasticity factor > 1 then elasticity is enabled
      // for each ssp create ElasticityFactor number of tasks
      // i.e; result will have number of tasks =  ElasticityFactor X number of SSP
      // each task will have name SSP[system,stream,partition,keyBucket] keyBucket <= elasticityFactor
      // each task portion corresponding to keyBucket of the SSP.
      for (int i = 0; i < elasticityFactor; i++) {
        int keyBucket = elasticityFactor == 1 ? -1 : i;
        SystemStreamPartition sspWithKeyBucket = new SystemStreamPartition(ssp, keyBucket);
        HashSet<SystemStreamPartition> sspSet = new HashSet<SystemStreamPartition>();
        sspSet.add(sspWithKeyBucket);
        groupedMap.put(new TaskName(sspWithKeyBucket.toString()), sspSet);
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
