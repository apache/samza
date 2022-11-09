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

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.GroupByContainerCountFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.chooser.RoundRobinChooserFactory;
import org.junit.Test;


public class TestTaskConfig {
  @Test
  public void testGetInputStreams() {
    Config config = new MapConfig(
        ImmutableMap.of(TaskConfig.INPUT_STREAMS, "kafka.foo, kafka.bar, otherKafka.bar, otherKafka.foo.bar"));
    Set<SystemStream> expected = ImmutableSet.of(
        new SystemStream("kafka", "foo"),
        new SystemStream("kafka", "bar"),
        new SystemStream("otherKafka", "bar"),
        new SystemStream("otherKafka", "foo.bar"));
    assertEquals(expected, new TaskConfig(config).getAllInputStreams());

    // empty string for value
    MapConfig configEmptyInput = new MapConfig(ImmutableMap.of(TaskConfig.INPUT_STREAMS, ""));
    assertTrue(new TaskConfig(configEmptyInput).getInputStreams().isEmpty());
    // config not specified
    assertTrue(new TaskConfig(new MapConfig()).getInputStreams().isEmpty());
  }

  @Test
  public void testGetWindowMs() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.WINDOW_MS, "10"));
    assertEquals(10, new TaskConfig(config).getWindowMs());

    config = new MapConfig(ImmutableMap.of(TaskConfig.WINDOW_MS, "-1"));
    assertEquals(-1, new TaskConfig(config).getWindowMs());

    // config not specified
    assertEquals(TaskConfig.DEFAULT_WINDOW_MS, new TaskConfig(new MapConfig()).getWindowMs());
  }

  @Test
  public void testGetCommitMs() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.COMMIT_MS, "10"));
    assertEquals(10, new TaskConfig(config).getCommitMs());

    config = new MapConfig(ImmutableMap.of(TaskConfig.COMMIT_MS, "-1"));
    assertEquals(-1, new TaskConfig(config).getCommitMs());

    // config not specified
    assertEquals(TaskConfig.DEFAULT_COMMIT_MS, new TaskConfig(new MapConfig()).getCommitMs());
  }

  @Test
  public void testGetTaskClass() {
    String taskClass = "some.task.class";
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.TASK_CLASS, taskClass));
    assertEquals(Optional.of(taskClass), new TaskConfig(config).getTaskClass());

    // config not specified
    assertFalse(new TaskConfig(new MapConfig()).getTaskClass().isPresent());
  }

  @Test
  public void testGetCommandClass() {
    String commandClass = "some.command.class";
    String defaultCommandClass = "default.command.class";
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.COMMAND_BUILDER, commandClass));
    assertEquals(commandClass, new TaskConfig(config).getCommandClass(defaultCommandClass));

    // config not specified
    assertEquals(defaultCommandClass, new TaskConfig(new MapConfig()).getCommandClass(defaultCommandClass));
  }

  @Test
  public void testGetMessageChooserClass() {
    String messageChooserClassValue = "some.message.chooser.class";
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.MESSAGE_CHOOSER_CLASS_NAME, messageChooserClassValue));
    assertEquals(messageChooserClassValue, new TaskConfig(config).getMessageChooserClass());

    // config not specified
    assertEquals(RoundRobinChooserFactory.class.getName(), new TaskConfig(new MapConfig()).getMessageChooserClass());
  }

  @Test
  public void testGetDropDeserializationErrors() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.DROP_DESERIALIZATION_ERRORS, "true"));
    assertTrue(new TaskConfig(config).getDropDeserializationErrors());

    config = new MapConfig(ImmutableMap.of(TaskConfig.DROP_DESERIALIZATION_ERRORS, "false"));
    assertFalse(new TaskConfig(config).getDropDeserializationErrors());

    // config not specified
    assertFalse(new TaskConfig(new MapConfig()).getDropDeserializationErrors());
  }

  @Test
  public void testGetDropSerializationErrors() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.DROP_SERIALIZATION_ERRORS, "true"));
    assertTrue(new TaskConfig(config).getDropSerializationErrors());

    config = new MapConfig(ImmutableMap.of(TaskConfig.DROP_SERIALIZATION_ERRORS, "false"));
    assertFalse(new TaskConfig(config).getDropSerializationErrors());

    // config not specified
    assertFalse(new TaskConfig(new MapConfig()).getDropSerializationErrors());
  }

  @Test
  public void testGetDropProducerErrors() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.DROP_PRODUCER_ERRORS, "true"));
    assertTrue(new TaskConfig(config).getDropProducerErrors());

    config = new MapConfig(ImmutableMap.of(TaskConfig.DROP_PRODUCER_ERRORS, "false"));
    assertFalse(new TaskConfig(config).getDropProducerErrors());

    // config not specified
    assertFalse(new TaskConfig(new MapConfig()).getDropProducerErrors());
  }

  @Test
  public void testGetPollIntervalMs() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.POLL_INTERVAL_MS, "10"));
    assertEquals(10, new TaskConfig(config).getPollIntervalMs());

    // config not specified
    assertEquals(TaskConfig.DEFAULT_POLL_INTERVAL_MS, new TaskConfig(new MapConfig()).getPollIntervalMs());
  }

  @Test
  public void testGetIgnoredExceptions() {
    String ignoredExceptionsValue = "exception0.class, exception1.class";
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.IGNORED_EXCEPTIONS, ignoredExceptionsValue));
    assertEquals(Optional.of(ignoredExceptionsValue), new TaskConfig(config).getIgnoredExceptions());

    // config not specified
    assertFalse(new TaskConfig(new MapConfig()).getIgnoredExceptions().isPresent());
  }

  @Test
  public void testGetTaskNameGrouperFactory() {
    String taskNameGrouperFactoryValue = "task.name.grouper.factory.class";
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.GROUPER_FACTORY, taskNameGrouperFactoryValue));
    assertEquals(taskNameGrouperFactoryValue, new TaskConfig(config).getTaskNameGrouperFactory());

    // config not specified
    assertEquals(GroupByContainerCountFactory.class.getName(),
        new TaskConfig(new MapConfig()).getTaskNameGrouperFactory());
  }

  @Test
  public void testGetMaxConcurrency() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.MAX_CONCURRENCY, "10"));
    assertEquals(10, new TaskConfig(config).getMaxConcurrency());

    // config not specified
    assertEquals(TaskConfig.DEFAULT_MAX_CONCURRENCY, new TaskConfig(new MapConfig()).getMaxConcurrency());
  }

  @Test
  public void testGetCallbackTimeoutMs() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.CALLBACK_TIMEOUT_MS, "10"));
    assertEquals(10, new TaskConfig(config).getCallbackTimeoutMs());

    config = new MapConfig(ImmutableMap.of(TaskConfig.CALLBACK_TIMEOUT_MS, "-1"));
    assertEquals(-1, new TaskConfig(config).getCallbackTimeoutMs());

    // config not specified
    assertEquals(TaskConfig.DEFAULT_CALLBACK_TIMEOUT_MS, new TaskConfig(new MapConfig()).getCallbackTimeoutMs());
  }

  @Test
  public void testGetDrainCallbackTimeoutMs() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.DRAIN_CALLBACK_TIMEOUT_MS, "100"));
    assertEquals(100, new TaskConfig(config).getDrainCallbackTimeoutMs());

    // config not specified
    assertEquals(TaskConfig.DEFAULT_DRAIN_CALLBACK_TIMEOUT_MS, new TaskConfig(new MapConfig()).getDrainCallbackTimeoutMs());
  }

  @Test
  public void testGetAsyncCommit() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.ASYNC_COMMIT, "true"));
    assertTrue(new TaskConfig(config).getAsyncCommit());

    config = new MapConfig(ImmutableMap.of(TaskConfig.ASYNC_COMMIT, "false"));
    assertFalse(new TaskConfig(config).getAsyncCommit());

    // config not specified
    assertFalse(new TaskConfig(new MapConfig()).getAsyncCommit());
  }

  @Test
  public void testGetMaxIdleMs() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.MAX_IDLE_MS, "20"));
    assertEquals(20, new TaskConfig(config).getMaxIdleMs());

    // config not specified
    assertEquals(TaskConfig.DEFAULT_MAX_IDLE_MS, new TaskConfig(new MapConfig()).getMaxIdleMs());
  }

  @Test
  public void testGetCheckpointManager() {
    Config config =
        new MapConfig(ImmutableMap.of(TaskConfig.CHECKPOINT_MANAGER_FACTORY, MockCheckpointManagerFactory.class.getName()));
    assertTrue(new TaskConfig(config).getCheckpointManager(null)
        .get() instanceof MockCheckpointManager);

    Config configEmptyString = new MapConfig(ImmutableMap.of(TaskConfig.CHECKPOINT_MANAGER_FACTORY, ""));
    assertFalse(new TaskConfig(configEmptyString).getCheckpointManager(null).isPresent());

    assertFalse(new TaskConfig(new MapConfig()).getCheckpointManager(null).isPresent());
  }

  @Test
  public void testGetBroadcastSystemStreamPartitions() {
    // no entry for "task.broadcast.inputs"
    assertEquals(Collections.emptySet(), new TaskConfig(new MapConfig()).getBroadcastSystemStreamPartitions());

    HashMap<String, String> map = new HashMap<>();
    map.put("task.broadcast.inputs", "kafka.foo#4, kafka.boo#5, kafka.z-o-o#[12-14], kafka.foo.bar#[3-4]");
    Config config = new MapConfig(map);
    TaskConfig taskConfig = new TaskConfig(config);
    Set<SystemStreamPartition> systemStreamPartitionSet = taskConfig.getBroadcastSystemStreamPartitions();

    HashSet<SystemStreamPartition> expected = new HashSet<>();
    expected.add(new SystemStreamPartition("kafka", "foo", new Partition(4)));
    expected.add(new SystemStreamPartition("kafka", "boo", new Partition(5)));
    expected.add(new SystemStreamPartition("kafka", "z-o-o", new Partition(12)));
    expected.add(new SystemStreamPartition("kafka", "z-o-o", new Partition(13)));
    expected.add(new SystemStreamPartition("kafka", "z-o-o", new Partition(14)));
    expected.add(new SystemStreamPartition("kafka", "foo.bar", new Partition(3)));
    expected.add(new SystemStreamPartition("kafka", "foo.bar", new Partition(4)));
    assertEquals(expected, systemStreamPartitionSet);

    map.put("task.broadcast.inputs", "kafka.foo");
    taskConfig = new TaskConfig(new MapConfig(map));
    boolean catchCorrectException = false;
    try {
      taskConfig.getBroadcastSystemStreamPartitions();
    } catch (IllegalArgumentException e) {
      catchCorrectException = true;
    }
    assertTrue(catchCorrectException);

    map.put("task.broadcast.inputs", "kafka.org.apache.events.WhitelistedIps#1-2");
    taskConfig = new TaskConfig(new MapConfig(map));
    boolean invalidFormatException = false;
    try {
      taskConfig.getBroadcastSystemStreamPartitions();
    } catch (IllegalArgumentException e) {
      invalidFormatException = true;
    }
    assertTrue(invalidFormatException);
  }

  @Test
  public void testGetBroadcastSystemStreams() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.BROADCAST_INPUT_STREAMS,
        "kafka.foo#4, kafka.bar#5, otherKafka.foo#4, otherKafka.foo.bar#5"));
    Set<SystemStream> expected = ImmutableSet.of(
        new SystemStream("kafka", "foo"),
        new SystemStream("kafka", "bar"),
        new SystemStream("otherKafka", "foo"),
        new SystemStream("otherKafka", "foo.bar"));
    assertEquals(expected, new TaskConfig(config).getBroadcastSystemStreams());
    assertTrue(new TaskConfig(new MapConfig()).getBroadcastSystemStreams().isEmpty());
  }

  @Test
  public void testGetAllInputStreams() {
    Config config = new MapConfig(ImmutableMap.of(
        TaskConfig.INPUT_STREAMS, "kafka.foo, otherKafka.bar",
        TaskConfig.BROADCAST_INPUT_STREAMS, "kafka.bar#4, otherKafka.foo#5"));
    Set<SystemStream> expected = ImmutableSet.of(
        new SystemStream("kafka", "foo"),
        new SystemStream("otherKafka", "bar"),
        new SystemStream("kafka", "bar"),
        new SystemStream("otherKafka", "foo"));
    assertEquals(expected, new TaskConfig(config).getAllInputStreams());

    Config configOnlyBroadcast = new MapConfig(ImmutableMap.of(
        TaskConfig.BROADCAST_INPUT_STREAMS, "kafka.bar#4, otherKafka.foo#5"));
    Set<SystemStream> expectedOnlyBroadcast = ImmutableSet.of(
        new SystemStream("kafka", "bar"),
        new SystemStream("otherKafka", "foo"));
    assertEquals(expectedOnlyBroadcast, new TaskConfig(configOnlyBroadcast).getAllInputStreams());

    Config configOnlyInputs = new MapConfig(ImmutableMap.of(TaskConfig.INPUT_STREAMS, "kafka.foo, otherKafka.bar"));
    Set<SystemStream> expectedOnlyInputs = ImmutableSet.of(
        new SystemStream("kafka", "foo"),
        new SystemStream("otherKafka", "bar"));
    assertEquals(expectedOnlyInputs, new TaskConfig(configOnlyInputs).getAllInputStreams());

    assertTrue(new TaskConfig(new MapConfig()).getAllInputStreams().isEmpty());
  }

  @Test
  public void testGetShutdownMs() {
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.TASK_SHUTDOWN_MS, "10"));
    assertEquals(10, new TaskConfig(config).getShutdownMs());

    // unable to parse value into number
    config = new MapConfig(ImmutableMap.of(TaskConfig.TASK_SHUTDOWN_MS, "not a number"));
    assertEquals(TaskConfig.DEFAULT_TASK_SHUTDOWN_MS, new TaskConfig(config).getShutdownMs());

    // config not specified
    assertEquals(TaskConfig.DEFAULT_TASK_SHUTDOWN_MS, new TaskConfig(new MapConfig()).getShutdownMs());
  }

  @Test
  public void testGetTransactionalStateRestoreEnabled() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(TaskConfig.TRANSACTIONAL_STATE_RESTORE_ENABLED, "true");

    // standby and async commit both off; transactional state restore returned as enabled
    assertTrue(new TaskConfig(new MapConfig(configMap)).getTransactionalStateRestoreEnabled());

    // standby off and async commit on; transactional state restore returned as disabled
    configMap.put(TaskConfig.ASYNC_COMMIT, "true");
    configMap.put(JobConfig.STANDBY_TASKS_REPLICATION_FACTOR, "1");
    assertFalse(new TaskConfig(new MapConfig(configMap)).getTransactionalStateRestoreEnabled());

    // standby on and async commit off; transactional state restore returned as disabled
    configMap.put(TaskConfig.ASYNC_COMMIT, "false");
    configMap.put(JobConfig.STANDBY_TASKS_REPLICATION_FACTOR, "2");
    assertFalse(new TaskConfig(new MapConfig(configMap)).getTransactionalStateRestoreEnabled());

    // standby on and async commit on; transactional state restore returned as disabled
    configMap.put(TaskConfig.ASYNC_COMMIT, "true");
    configMap.put(JobConfig.STANDBY_TASKS_REPLICATION_FACTOR, "2");
    assertFalse(new TaskConfig(new MapConfig(configMap)).getTransactionalStateRestoreEnabled());
  }

  /**
   * Used for testing classloading a {@link CheckpointManagerFactory}.
   */
  public static class MockCheckpointManagerFactory implements CheckpointManagerFactory {
    @Override
    public CheckpointManager getCheckpointManager(Config config, MetricsRegistry registry) {
      return new MockCheckpointManager();
    }
  }

  /**
   * Placeholder class to be returned by {@link MockCheckpointManagerFactory}.
   */
  private static class MockCheckpointManager implements CheckpointManager {
    @Override
    public void start() { }

    @Override
    public void register(TaskName taskName) { }

    @Override
    public void writeCheckpoint(TaskName taskName, Checkpoint checkpoint) { }

    @Override
    public Checkpoint readLastCheckpoint(TaskName taskName) {
      return null;
    }

    @Override
    public void stop() { }
  }
}
