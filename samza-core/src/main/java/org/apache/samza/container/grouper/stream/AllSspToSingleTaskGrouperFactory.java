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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;


/**
 * AllSspToSingleTaskGrouper creates TaskInstances equal to the number of containers and assigns all partitions to be
 * consumed by each TaskInstance. This is useful, in case of using load-balanced consumers like the high-level Kafka
 * consumer and Kinesis consumer, where Samza doesn't control the partitions being consumed by the task.
 *
 * Note that this grouper does not take in broadcast streams yet.
 */

class AllSspToSingleTaskGrouper implements SystemStreamPartitionGrouper {
  private final List<String> processorList;

  public AllSspToSingleTaskGrouper(List<String> processorList) {
    this.processorList = processorList;
  }

  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(final Set<SystemStreamPartition> ssps) {
    Map<TaskName, Set<SystemStreamPartition>> groupedMap = new HashMap<>();

    if (ssps == null) {
      throw new SamzaException("ssp set cannot be null!");
    }
    if (ssps.size() == 0) {
      throw new SamzaException("Cannot process stream task with no input system stream partitions");
    }

    processorList.forEach(processor -> {
        // Create a task name for each processor and assign all partitions to each task name.
        final TaskName taskName = new TaskName(String.format("Task-%s", processor));
        groupedMap.put(taskName, ssps);
      });

    return groupedMap;
  }
}

public class AllSspToSingleTaskGrouperFactory implements SystemStreamPartitionGrouperFactory {
  @Override
  public SystemStreamPartitionGrouper getSystemStreamPartitionGrouper(Config config) {
    if (!(new TaskConfigJava(config).getBroadcastSystemStreams().isEmpty())) {
      throw new ConfigException("The job configured with AllSspToSingleTaskGrouper cannot have broadcast streams.");
    }

    String processors = config.get(JobConfig.PROCESSOR_LIST());
    List<String> processorList = Arrays.asList(processors.split(","));
    return new AllSspToSingleTaskGrouper(processorList);
  }
}