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

import com.google.common.collect.ImmutableList;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.container.grouper.task.GroupByContainerCountFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.chooser.RoundRobinChooserFactory;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.StreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskConfig extends MapConfig {
  public static final Logger LOGGER = LoggerFactory.getLogger(TaskConfig.class);

  // comma-separated list of system-streams
  public static final String INPUT_STREAMS = "task.inputs";
  // window period in milliseconds
  public static final String WINDOW_MS = "task.window.ms";
  static final long DEFAULT_WINDOW_MS = -1L;
  // commit period in milliseconds
  public static final String COMMIT_MS = "task.commit.ms";
  static final long DEFAULT_COMMIT_MS = 60000L;
  // maximum amount of time a task may continue processing while a previous commit is pending
  public static final String COMMIT_MAX_DELAY_MS = "task.commit.max.delay.ms";
  static final long DEFAULT_COMMIT_MAX_DELAY_MS = Duration.ofMinutes(1).toMillis();
  // maximum amount of time to block for a pending task commit to complete *after*
  // COMMIT_MAX_DELAY_MS have passed since the pending commit start. if the pending commit
  // does not complete within this timeout, the container will shut down.
  public static final String COMMIT_TIMEOUT_MS = "task.commit.timeout.ms";
  static final long DEFAULT_COMMIT_TIMEOUT_MS = Duration.ofMinutes(30).toMillis();

  // how long to wait for a clean shutdown
  public static final String TASK_SHUTDOWN_MS = "task.shutdown.ms";
  static final long DEFAULT_TASK_SHUTDOWN_MS = 30000L;
  // legacy config for specifying task class; replaced by SamzaApplication and app.class
  public static final String TASK_CLASS = "task.class";
  // command builder to use for launching a Samza job
  public static final String COMMAND_BUILDER = "task.command.class";
  // message chooser for controlling stream consumption
  public static final String MESSAGE_CHOOSER_CLASS_NAME = "task.chooser.class";
  // define whether to drop the messages or not when deserialization fails
  public static final String DROP_DESERIALIZATION_ERRORS = "task.drop.deserialization.errors";
  // define whether to drop the messages or not when serialization fails
  public static final String DROP_SERIALIZATION_ERRORS = "task.drop.serialization.errors";
  // whether to ignore producer errors and drop the messages that failed to send
  public static final String DROP_PRODUCER_ERRORS = "task.drop.producer.errors";
  // exceptions to ignore in process and window
  public static final String IGNORED_EXCEPTIONS = "task.ignored.exceptions";
  // class name for task grouper
  public static final String GROUPER_FACTORY = "task.name.grouper.factory";
  // max number of messages to process concurrently
  public static final String MAX_CONCURRENCY = "task.max.concurrency";
  static final int DEFAULT_MAX_CONCURRENCY = 1;
  // timeout for triggering a callback
  public static final String CALLBACK_TIMEOUT_MS = "task.callback.timeout.ms";
  static final long DEFAULT_CALLBACK_TIMEOUT_MS = -1L;

  // timeout for triggering a callback during drain
  public static final String DRAIN_CALLBACK_TIMEOUT_MS = "task.callback.drain.timeout.ms";

  // default timeout for triggering a callback during drain
  static final long DEFAULT_DRAIN_CALLBACK_TIMEOUT_MS = -1L;

  // enable async commit
  public static final String ASYNC_COMMIT = "task.async.commit";
  // maximum time to wait for a task worker to complete when there are no new messages to handle
  public static final String MAX_IDLE_MS = "task.max.idle.ms";
  static final long DEFAULT_MAX_IDLE_MS = 10L;
  /**
   * Samza's container polls for more messages under two conditions. The first
   * condition arises when there are simply no remaining buffered messages to
   * process for any input SystemStreamPartition. The second condition arises
   * when some input SystemStreamPartitions have empty buffers, but some do
   * not. In the latter case, a polling interval is defined to determine how
   * often to refresh the empty SystemStreamPartition buffers. By default,
   * this interval is 50ms, which means that any empty SystemStreamPartition
   * buffer will be refreshed at least every 50ms. A higher value here means
   * that empty SystemStreamPartitions will be refreshed less often, which
   * means more latency is introduced, but less CPU and network will be used.
   * Decreasing this value means that empty SystemStreamPartitions are
   * refreshed more frequently, thereby introducing less latency, but
   * increasing CPU and network utilization.
   */
  public static final String POLL_INTERVAL_MS = "task.poll.interval.ms";
  public static final int DEFAULT_POLL_INTERVAL_MS = 50;
  // broadcast streams consumed by all tasks. e.g. kafka.foo#1
  public static final String BROADCAST_INPUT_STREAMS = "task.broadcast.inputs";
  private static final String BROADCAST_STREAM_PATTERN = "^[\\d]+$";
  private static final String BROADCAST_STREAM_RANGE_PATTERN = "^\\[[\\d]+\\-[\\d]+\\]$";
  public static final String CHECKPOINT_MANAGER_FACTORY = "task.checkpoint.factory";
  // standby containers use this flag to indicate that checkpoints will be polled continually, rather than only once at startup like in an active container
  public static final String INTERNAL_CHECKPOINT_MANAGER_CONSUMER_STOP_AFTER_FIRST_READ = "samza.internal.task.checkpoint.consumer.stop.after.first.read";

  // list of checkpoint versions to write during processing
  public static final String CHECKPOINT_WRITE_VERSIONS = "task.checkpoint.write.versions";
  public static final List<String> DEFAULT_CHECKPOINT_WRITE_VERSIONS = ImmutableList.of("1", "2");

  // checkpoint version to read during container startup
  public static final String CHECKPOINT_READ_VERSIONS = "task.checkpoint.read.versions";
  public static final List<String> DEFAULT_CHECKPOINT_READ_VERSIONS = ImmutableList.of("1");
  public static final String LIVE_CHECKPOINT_MAX_AGE_MS = "task.live.checkpoint.max.age";
  public static final long DEFAULT_LIVE_CHECKPOINT_MAX_AGE_MS = 600000L; // 10 mins

  public static final String TRANSACTIONAL_STATE_CHECKPOINT_ENABLED = "task.transactional.state.checkpoint.enabled";
  private static final boolean DEFAULT_TRANSACTIONAL_STATE_CHECKPOINT_ENABLED = true;
  public static final String TRANSACTIONAL_STATE_RESTORE_ENABLED = "task.transactional.state.restore.enabled";
  private static final boolean DEFAULT_TRANSACTIONAL_STATE_RESTORE_ENABLED = true;
  public static final String TRANSACTIONAL_STATE_RETAIN_EXISTING_STATE =
      "task.transactional.state.retain.existing.state";
  private static final boolean DEFAULT_TRANSACTIONAL_STATE_RETAIN_EXISTING_STATE = true;

  public TaskConfig(Config config) {
    super(config);
  }

  /**
   * Get the input streams, not including the broadcast streams. Use {@link #getAllInputStreams()} to also get the
   * broadcast streams.
   */
  public Set<SystemStream> getInputStreams() {
    Optional<String> inputStreams = Optional.ofNullable(get(INPUT_STREAMS));
    if (!inputStreams.isPresent() || inputStreams.get().isEmpty()) {
      return Collections.emptySet();
    } else {
      return Stream.of(inputStreams.get().split(","))
          .map(systemStreamNames -> StreamUtil.getSystemStreamFromNames(systemStreamNames.trim()))
          .collect(Collectors.toSet());
    }
  }

  public long getWindowMs() {
    return getLong(WINDOW_MS, DEFAULT_WINDOW_MS);
  }

  public long getCommitMs() {
    return getLong(COMMIT_MS, DEFAULT_COMMIT_MS);
  }

  public long getCommitMaxDelayMs() {
    return getLong(COMMIT_MAX_DELAY_MS, DEFAULT_COMMIT_MAX_DELAY_MS);
  }

  public long getCommitTimeoutMs() {
    return getLong(COMMIT_TIMEOUT_MS, DEFAULT_COMMIT_TIMEOUT_MS);
  }

  public Optional<String> getTaskClass() {
    return Optional.ofNullable(get(TASK_CLASS));
  }

  public String getCommandClass(String defaultCommandClass) {
    return get(COMMAND_BUILDER, defaultCommandClass);
  }

  public String getMessageChooserClass() {
    return Optional.ofNullable(get(MESSAGE_CHOOSER_CLASS_NAME)).orElse(RoundRobinChooserFactory.class.getName());
  }

  public boolean getDropDeserializationErrors() {
    return getBoolean(DROP_DESERIALIZATION_ERRORS, false);
  }

  public boolean getDropSerializationErrors() {
    return getBoolean(DROP_SERIALIZATION_ERRORS, false);
  }

  public boolean getDropProducerErrors() {
    return getBoolean(DROP_PRODUCER_ERRORS, false);
  }

  public int getPollIntervalMs() {
    return getInt(POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
  }

  public Optional<String> getIgnoredExceptions() {
    return Optional.ofNullable(get(IGNORED_EXCEPTIONS));
  }

  public String getTaskNameGrouperFactory() {
    Optional<String> taskNameGrouperFactory = Optional.ofNullable(get(GROUPER_FACTORY));
    if (taskNameGrouperFactory.isPresent()) {
      return taskNameGrouperFactory.get();
    } else {
      LOGGER.info(String.format("No %s configuration, using %s", GROUPER_FACTORY,
          GroupByContainerCountFactory.class.getName()));
      return GroupByContainerCountFactory.class.getName();
    }
  }

  public int getMaxConcurrency() {
    return getInt(MAX_CONCURRENCY, DEFAULT_MAX_CONCURRENCY);
  }

  public long getCallbackTimeoutMs() {
    return getLong(CALLBACK_TIMEOUT_MS, DEFAULT_CALLBACK_TIMEOUT_MS);
  }

  public long getDrainCallbackTimeoutMs() {
    return getLong(DRAIN_CALLBACK_TIMEOUT_MS, DEFAULT_DRAIN_CALLBACK_TIMEOUT_MS);
  }

  public boolean getAsyncCommit() {
    return getBoolean(ASYNC_COMMIT, false);
  }

  public long getMaxIdleMs() {
    return getLong(MAX_IDLE_MS, DEFAULT_MAX_IDLE_MS);
  }

  /**
   * Create the checkpoint manager
   *
   * @param metricsRegistry Registry of metrics to use. Can be null if not using metrics.
   * @return CheckpointManager object if checkpoint manager factory is configured, otherwise empty.
   */
  public Optional<CheckpointManager> getCheckpointManager(MetricsRegistry metricsRegistry) {
    return Optional.ofNullable(get(CHECKPOINT_MANAGER_FACTORY))
        .filter(StringUtils::isNotBlank)
        .map(checkpointManagerFactoryName -> ReflectionUtil.getObj(checkpointManagerFactoryName,
            CheckpointManagerFactory.class).getCheckpointManager(this, metricsRegistry));
  }

  /**
   * Internal config to indicate whether the SystemConsumer underlying a CheckpointManager should be stopped after
   * initial read of checkpoints.
   */
  public boolean getCheckpointManagerConsumerStopAfterFirstRead() {
    return getBoolean(INTERNAL_CHECKPOINT_MANAGER_CONSUMER_STOP_AFTER_FIRST_READ, true);
  }

  /**
   * Get the systemStreamPartitions of the broadcast stream. Specifying
   * one partition for one stream or a range of the partitions for one
   * stream is allowed.
   *
   * @return a Set of SystemStreamPartitions
   */
  public Set<SystemStreamPartition> getBroadcastSystemStreamPartitions() {
    HashSet<SystemStreamPartition> systemStreamPartitionSet = new HashSet<>();
    List<String> systemStreamPartitions = getList(BROADCAST_INPUT_STREAMS, Collections.emptyList());

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

    allInputSS.addAll(getInputStreams());
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

  public List<Short> getCheckpointWriteVersions() {
    return getList(CHECKPOINT_WRITE_VERSIONS, DEFAULT_CHECKPOINT_WRITE_VERSIONS)
        .stream().map(Short::valueOf).collect(Collectors.toList());
  }

  public List<Short> getCheckpointReadVersions() {
    List<Short> checkpointReadPriorityList = getList(CHECKPOINT_READ_VERSIONS, DEFAULT_CHECKPOINT_READ_VERSIONS)
        .stream().map(Short::valueOf).collect(Collectors.toList());
    if (checkpointReadPriorityList.isEmpty()) {
      // if the user explicitly defines the checkpoint read list to be empty
      throw new IllegalArgumentException("No checkpoint read versions defined for job. "
          + "Please remove the task.checkpoint.read.versions or define valid checkpoint versions");
    } else {
      return checkpointReadPriorityList;
    }
  }

  public long getLiveCheckpointMaxAgeMillis() {
    return getLong(LIVE_CHECKPOINT_MAX_AGE_MS, DEFAULT_LIVE_CHECKPOINT_MAX_AGE_MS);
  }

  public boolean getTransactionalStateCheckpointEnabled() {
    return getBoolean(TRANSACTIONAL_STATE_CHECKPOINT_ENABLED, DEFAULT_TRANSACTIONAL_STATE_CHECKPOINT_ENABLED);
  }

  public boolean getTransactionalStateRestoreEnabled() {
    JobConfig jobConfig = new JobConfig(this);

    boolean standByEnabled = jobConfig.getStandbyTasksEnabled();
    boolean asyncCommitEnabled = getAsyncCommit();

    // TODO remove check of standby enabled when SAMZA-2353 is completed
    // TODO remove check of async commit when SAMZA-2505 is completed
    // transactional state restore must remain disabled until it is supported in the above use cases
    return !standByEnabled && !asyncCommitEnabled && getBoolean(TRANSACTIONAL_STATE_RESTORE_ENABLED, DEFAULT_TRANSACTIONAL_STATE_RESTORE_ENABLED);
  }

  public boolean getTransactionalStateRetainExistingState() {
    return getBoolean(TRANSACTIONAL_STATE_RETAIN_EXISTING_STATE, DEFAULT_TRANSACTIONAL_STATE_RETAIN_EXISTING_STATE);
  }
}
