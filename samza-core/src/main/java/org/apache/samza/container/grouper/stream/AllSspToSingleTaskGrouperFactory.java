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
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * AllSspToSingleTaskGrouper, as the name suggests, assigns all partitions to be consumed by a single TaskInstance
 * This is useful, in case of using load-balanced consumers like the new Kafka consumer, Samza doesn't control the
 * partitions being consumed by a task. Hence, it is assumed that there is only 1 task that processes all messages,
 * irrespective of which partition it belongs to.
 * This also implies that container and tasks are synonymous when this grouper is used. Taskname(s) has to be globally
 * unique within a given job.
 *
 * Note: This grouper does not take in broadcast streams yet.
 */
class AllSspToSingleTaskGrouper implements SystemStreamPartitionGrouper {
  private final int containerId;

  public AllSspToSingleTaskGrouper(int containerId) {
    this.containerId = containerId;
  }

  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> sspSet) {
    return group(new HashMap<>(), sspSet);
  }

  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(Map<SystemStreamPartition, String> previousSSPTaskAssignment,
                                                         Set<SystemStreamPartition> sspSet) {
    if (sspSet == null) {
      throw new SamzaException("ssp set cannot be null!");
    }
    if (sspSet.size() == 0) {
      throw new SamzaException("Cannot process stream task with no input system stream partitions");
    }

    final TaskName taskName = new TaskName(String.format("Task-%s", String.valueOf(containerId)));

    return Collections.singletonMap(taskName, sspSet);
  }
}

public class AllSspToSingleTaskGrouperFactory implements SystemStreamPartitionGrouperFactory {
  @Override
  public SystemStreamPartitionGrouper getSystemStreamPartitionGrouper(Config config) {
    return new AllSspToSingleTaskGrouper(config.getInt(JobConfig.PROCESSOR_ID()));
  }
}