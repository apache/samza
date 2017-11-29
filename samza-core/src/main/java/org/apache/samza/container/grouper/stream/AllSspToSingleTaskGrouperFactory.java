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
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.apache.samza.zk.ZkKeyBuilder;

import static org.apache.samza.zk.ZkCoordinationUtilsFactory.*;
import static org.apache.samza.zk.ZkJobCoordinatorFactory.*;


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
  private final boolean isPassthroughJobCoordinator;
  private final int taskCount;
  private final int containerId;

  AllSspToSingleTaskGrouper(boolean isPassthroughJobCoordinator, int containerId, int taskCount) {
    this.isPassthroughJobCoordinator = isPassthroughJobCoordinator;
    this.containerId = containerId;
    this.taskCount = taskCount;
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

    if (isPassthroughJobCoordinator) {
      final TaskName taskName = new TaskName(String.format("Task-%d", containerId));
      groupedMap.put(taskName, ssps);
    } else {
      // Assign all partitions to each task
      IntStream.range(0, taskCount).forEach(i -> {
          final TaskName taskName = new TaskName(String.format("Task-%s", String.valueOf(i)));
          groupedMap.put(taskName, ssps);
        });
    }

    return groupedMap;
  }
}

public class AllSspToSingleTaskGrouperFactory implements SystemStreamPartitionGrouperFactory {
  private static final String YARN_CONTAINER_COUNT = "yarn.container.count";
  private static final String PASSTHROUGH_JOB_COORDINATOR_FACTORY = PassthroughJobCoordinatorFactory.class.getName();
  private static final String ZK_JOB_COORDINATOR_FACTORY = ZkJobCoordinatorFactory.class.getName();

  @Override
  public SystemStreamPartitionGrouper getSystemStreamPartitionGrouper(Config config) {
    int taskCount;
    int containerId = 0;
    boolean isPassthroughJobCoordinator = false;
    String jobCoordinatorFactoryClassName = config.get(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY);

    if (jobCoordinatorFactoryClassName != null && !jobCoordinatorFactoryClassName.isEmpty()) {
      if (jobCoordinatorFactoryClassName.equals(ZK_JOB_COORDINATOR_FACTORY)) {
        taskCount = getNumProcessors(config);
      } else if (jobCoordinatorFactoryClassName.equals(PASSTHROUGH_JOB_COORDINATOR_FACTORY)) {
        if (config.get(JobConfig.PROCESSOR_ID()) == null) {
          throw new ConfigException("Could not find " + JobConfig.PROCESSOR_ID() + " in Config!");
        }
        isPassthroughJobCoordinator = true;
        containerId = config.getInt(JobConfig.PROCESSOR_ID());
        taskCount = 1;
      } else {
        throw new ConfigException("AllSspToSingleTaskGrouperFactory is not yet supported with "
            + jobCoordinatorFactoryClassName);
      }
    } else {
      // must be cluster-based jobCoordinator (Yarn)
      taskCount = config.getInt(YARN_CONTAINER_COUNT);
    }

    Validate.isTrue(taskCount > 0, "Task count cannot be <= 0");

    return new AllSspToSingleTaskGrouper(isPassthroughJobCoordinator, containerId, taskCount);
  }

  private int getNumProcessors(Config config) {
    ZkConfig zkConfig = new ZkConfig(config);
    ZkClient zkClient =
        createZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());
    ZkKeyBuilder keyBuilder = new ZkKeyBuilder(getJobCoordinationZkPath(config));
    return zkClient.countChildren(keyBuilder.getProcessorsPath());
  }
}