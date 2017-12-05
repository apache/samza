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
import java.util.stream.IntStream;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * AllSspToSingleTaskGrouper creates TaskInstances equal to the number of containers and assigns all partitions to be
 * consumed by each TaskInstance. This is useful, in case of using load-balanced consumers like the high-level Kafka
 * consumer and Kinesis consumer, where Samza doesn't control the partitions being consumed by the task.
 *
 * Note:
 * 1. This grouper does not take in broadcast streams yet.
 * 2. This grouper is supported only with Yarn, Zk-standalone and Passthrough JobCoordinators.
 */

class AllSspToSingleTaskGrouper implements SystemStreamPartitionGrouper {
  private final Set<String> processorNames;

  AllSspToSingleTaskGrouper(Set<String> processorNames) {
    this.processorNames = processorNames;
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

    processorNames.forEach(processorName -> {
        // Create a task name for each processor and assign all partitions to each task name.
        final TaskName taskName = new TaskName(String.format("Task-%s", processorName));
        groupedMap.put(taskName, ssps);
      });

    return groupedMap;
  }
}

public class AllSspToSingleTaskGrouperFactory implements SystemStreamPartitionGrouperFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AllSspToSingleTaskGrouperFactory.class);

  @Override
  public SystemStreamPartitionGrouper getSystemStreamPartitionGrouper(Config config) {
    if (!(new TaskConfigJava(config).getBroadcastSystemStreams().isEmpty())) {
      throw new ConfigException("The job configured with AllSspToSingleTaskGrouper cannot have broadcast streams.");
    }

    try {
      String jobCoordinatorFactoryClassName = new JobCoordinatorConfig(config).getJobCoordinatorFactoryClassName();
      // TODO: SAMZA-1521 We need to implement a JobCoordinator Observer which would just read the state of the
      // JobCoordinator instead of acting as a participant. Please do not call any other APIs on the JobCoordinator
      // below.
      JobCoordinator jc = Util.<JobCoordinatorFactory>getObj(jobCoordinatorFactoryClassName).getJobCoordinator(config);
      return new AllSspToSingleTaskGrouper(jc.getProcessorNames());
    } catch (ConfigException ex) {
      LOG.info("Guessing cluster-based jobCoordinator(Yarn).");
      final Set<String> processorNames = new HashSet<>();
      IntStream.range(0, new JobConfig(config).getContainerCount()).forEach(i -> processorNames.add(String.valueOf(i)));
      return new AllSspToSingleTaskGrouper(processorNames);
    }
  }
}