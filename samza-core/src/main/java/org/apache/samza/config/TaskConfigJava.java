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

package org.apache.samza.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.StreamUtil;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


public class TaskConfigJava extends MapConfig {
  // Task Configs
  public static final String TASK_SHUTDOWN_MS = "task.shutdown.ms";
  public static final long DEFAULT_TASK_SHUTDOWN_MS = 30000L;

  // broadcast streams consumed by all tasks. e.g. kafka.foo#1
  public static final String BROADCAST_INPUT_STREAMS = "task.broadcast.inputs";
  private static final String BROADCAST_STREAM_PATTERN = "^[\\d]+$";
  private static final String BROADCAST_STREAM_RANGE_PATTERN = "^\\[[\\d]+\\-[\\d]+\\]$";

  // class name to use when sending offset checkpoints
  public static final String CHECKPOINT_MANAGER_FACTORY = "task.checkpoint.factory";

  public static final Logger LOGGER = LoggerFactory.getLogger(TaskConfigJava.class);

  public TaskConfigJava(Config config) {
    super(config);
  }

  /**
   * Get the name of the checkpoint manager factory
   *
   * @return Name of checkpoint manager factory
   */
  public String getCheckpointManagerFactoryName() {
    return get(CHECKPOINT_MANAGER_FACTORY, null);
  }

  /**
   * Create the checkpoint manager
   *
   * @param metricsRegistry Registry of metrics to use. Can be null if not using metrics.
   * @return CheckpointManager object if checkpoint manager factory is configured, otherwise null.
   */
  public CheckpointManager getCheckpointManager(MetricsRegistry metricsRegistry) {
    // Initialize checkpoint streams during job coordination
    String checkpointManagerFactoryName = getCheckpointManagerFactoryName();
    if (StringUtils.isNotBlank(checkpointManagerFactoryName)) {
      CheckpointManager checkpointManager =
          Util.getObj(checkpointManagerFactoryName, CheckpointManagerFactory.class).getCheckpointManager(this, metricsRegistry);
      return checkpointManager;
    }
    return null;
  }

  /**
   * Get the systemStreamPartitions of the broadcast stream. Specifying
   * one partition for one stream or a range of the partitions for one
   * stream is allowed.
   *
   * @return a Set of SystemStreamPartitions
   */
  public Set<SystemStreamPartition> getBroadcastSystemStreamPartitions() {
    HashSet<SystemStreamPartition> systemStreamPartitionSet = new HashSet<SystemStreamPartition>();
    List<String> systemStreamPartitions = getList(BROADCAST_INPUT_STREAMS, Collections.<String>emptyList());

    for (String systemStreamPartition : systemStreamPartitions) {
      int hashPosition = systemStreamPartition.indexOf("#");
      if (hashPosition == -1) {
        throw new IllegalArgumentException("incorrect format in " + systemStreamPartition
            + ". Broadcast stream names should be in the form 'system.stream#partitionId' or 'system.stream#[partitionN-partitionM]'");
      } else {
        String systemStreamName = systemStreamPartition.substring(0, hashPosition);
        String partitionSegment = systemStreamPartition.substring(hashPosition + 1);
        SystemStream systemStream = StreamUtil.getSystemStreamFromNames(systemStreamName);

        if (Pattern.matches(BROADCAST_STREAM_PATTERN, partitionSegment)) {
          systemStreamPartitionSet.add(new SystemStreamPartition(systemStream, new Partition(Integer.valueOf(partitionSegment))));
        } else {
          if (Pattern.matches(BROADCAST_STREAM_RANGE_PATTERN, partitionSegment)) {
            int partitionStart = Integer.valueOf(partitionSegment.substring(1, partitionSegment.lastIndexOf("-")));
            int partitionEnd = Integer.valueOf(partitionSegment.substring(partitionSegment.lastIndexOf("-") + 1, partitionSegment.indexOf("]")));
            if (partitionStart > partitionEnd) {
              LOGGER.warn("The starting partition in stream " + systemStream.toString() + " is bigger than the ending Partition. No partition is added");
            }
            for (int i = partitionStart; i <= partitionEnd; i++) {
              systemStreamPartitionSet.add(new SystemStreamPartition(systemStream, new Partition(i)));
            }
          } else {
            throw new IllegalArgumentException("incorrect format in " + systemStreamPartition
                + ". Broadcast stream names should be in the form 'system.stream#partitionId' or 'system.stream#[partitionN-partitionM]'");
          }
        }
      }
    }
    return systemStreamPartitionSet;
  }

  /**
   * Get the SystemStreams for the configured broadcast streams.
   *
   * @return the set of SystemStreams for which there are broadcast stream SSPs configured.
   */
  public Set<SystemStream> getBroadcastSystemStreams() {
    Set<SystemStream> broadcastSS = new HashSet<>();
    Set<SystemStreamPartition> broadcastSSPs = getBroadcastSystemStreamPartitions();
    for (SystemStreamPartition bssp : broadcastSSPs) {
      broadcastSS.add(bssp.getSystemStream());
    }
    return Collections.unmodifiableSet(broadcastSS);
  }

  /**
   * Get the SystemStreams for the configured input and broadcast streams.
   *
   * @return the set of SystemStreams for both standard inputs and broadcast stream inputs.
   */
  public Set<SystemStream> getAllInputStreams() {
    Set<SystemStream> allInputSS = new HashSet<>();

    TaskConfig taskConfig = TaskConfig.Config2Task(this);
    allInputSS.addAll((Set<? extends SystemStream>) JavaConverters.setAsJavaSetConverter(taskConfig.getInputStreams()).asJava());
    allInputSS.addAll(getBroadcastSystemStreams());

    return Collections.unmodifiableSet(allInputSS);
  }

  /**
   * Returns a value indicating how long to wait for the tasks to shutdown
   * If the value is not defined in the config or if does not parse correctly, we return the default value -
   * {@value #DEFAULT_TASK_SHUTDOWN_MS}
   *
   * @return Long value indicating how long to wait for all the tasks to shutdown
   */
  public long getShutdownMs() {
    String shutdownMs = get(TASK_SHUTDOWN_MS);
    try {
      return Long.parseLong(shutdownMs);
    } catch (NumberFormatException nfe) {
      LOGGER.warn(String.format(
          "Unable to parse user-configure value for %s - %s. Using default value %d",
          TASK_SHUTDOWN_MS,
          shutdownMs,
          DEFAULT_TASK_SHUTDOWN_MS));
      return DEFAULT_TASK_SHUTDOWN_MS;
    }
  }
}
