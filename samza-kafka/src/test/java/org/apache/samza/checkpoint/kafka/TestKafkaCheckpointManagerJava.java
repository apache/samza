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
package org.apache.samza.checkpoint.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import kafka.common.KafkaException;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.CheckpointSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.kafka.KafkaStreamSpec;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

public class TestKafkaCheckpointManagerJava {
  private static final TaskName TASK1 = new TaskName("task1");
  private static final String CHECKPOINT_TOPIC = "topic-1";
  private static final String CHECKPOINT_SYSTEM = "system-1";
  private static final Partition CHECKPOINT_PARTITION = new Partition(0);
  private static final SystemStreamPartition CHECKPOINT_SSP =
      new SystemStreamPartition(CHECKPOINT_SYSTEM, CHECKPOINT_TOPIC, CHECKPOINT_PARTITION);
  private static final String GROUPER_FACTORY_CLASS = GroupByPartitionFactory.class.getCanonicalName();

  @Test(expected = TopicAlreadyMarkedForDeletionException.class)
  public void testStartFailsOnTopicCreationErrors() {

    KafkaStreamSpec checkpointSpec = new KafkaStreamSpec(CHECKPOINT_TOPIC, CHECKPOINT_TOPIC,
        CHECKPOINT_SYSTEM, 1);
    // create an admin that throws an exception during createStream
    SystemAdmin mockAdmin = newAdmin("0", "10");
    doThrow(new TopicAlreadyMarkedForDeletionException("invalid stream")).when(mockAdmin).createStream(checkpointSpec);

    SystemFactory factory = newFactory(mock(SystemProducer.class), mock(SystemConsumer.class), mockAdmin);
    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(checkpointSpec, factory,
        true, mock(Config.class), mock(MetricsRegistry.class), null);

    // expect an exception during startup
    checkpointManager.start();
  }

  @Test(expected = StreamValidationException.class)
  public void testStartFailsOnTopicValidationErrors() {

    KafkaStreamSpec checkpointSpec = new KafkaStreamSpec(CHECKPOINT_TOPIC, CHECKPOINT_TOPIC,
        CHECKPOINT_SYSTEM, 1);

    // create an admin that throws an exception during validateStream
    SystemAdmin mockAdmin = newAdmin("0", "10");
    doThrow(new StreamValidationException("invalid stream")).when(mockAdmin).validateStream(checkpointSpec);

    SystemFactory factory = newFactory(mock(SystemProducer.class), mock(SystemConsumer.class), mockAdmin);
    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(checkpointSpec, factory,
        true, mock(Config.class), mock(MetricsRegistry.class), null);

    // expect an exception during startup
    checkpointManager.start();
  }

  @Test(expected = KafkaException.class)
  public void testReadFailsOnSerdeExceptions() throws Exception {
    KafkaStreamSpec checkpointSpec = new KafkaStreamSpec(CHECKPOINT_TOPIC, CHECKPOINT_TOPIC,
        CHECKPOINT_SYSTEM, 1);
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(JobConfig.SSP_GROUPER_FACTORY())).thenReturn(GROUPER_FACTORY_CLASS);

    // mock out a consumer that returns a single checkpoint IME
    SystemStreamPartition ssp = new SystemStreamPartition("system-1", "input-topic", new Partition(0));
    List<List<IncomingMessageEnvelope>> checkpointEnvelopes = ImmutableList.of(
        ImmutableList.of(newCheckpointEnvelope(TASK1, ssp, "0")));
    SystemConsumer mockConsumer = newConsumer(checkpointEnvelopes);

    SystemAdmin mockAdmin = newAdmin("0", "1");
    SystemFactory factory = newFactory(mock(SystemProducer.class), mockConsumer, mockAdmin);

    // wire up an exception throwing serde with the checkpointmanager
    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(checkpointSpec, factory,
        true, mockConfig, mock(MetricsRegistry.class), new ExceptionThrowingCheckpointSerde());
    checkpointManager.register(TASK1);
    checkpointManager.start();

    // expect an exception from ExceptionThrowingSerde
    checkpointManager.readLastCheckpoint(TASK1);
  }

  @Test
  public void testCheckpointsAreReadFromOldestOffset() throws Exception {
    KafkaStreamSpec checkpointSpec = new KafkaStreamSpec(CHECKPOINT_TOPIC, CHECKPOINT_TOPIC,
        CHECKPOINT_SYSTEM, 1);
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(JobConfig.SSP_GROUPER_FACTORY())).thenReturn(GROUPER_FACTORY_CLASS);

    // mock out a consumer that returns a single checkpoint IME
    SystemStreamPartition ssp = new SystemStreamPartition("system-1", "input-topic", new Partition(0));
    SystemConsumer mockConsumer = newConsumer(ImmutableList.of(
        ImmutableList.of(newCheckpointEnvelope(TASK1, ssp, "0"))));

    String oldestOffset = "0";
    SystemAdmin mockAdmin = newAdmin(oldestOffset, "1");
    SystemFactory factory = newFactory(mock(SystemProducer.class), mockConsumer, mockAdmin);
    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(checkpointSpec, factory,
        true, mockConfig, mock(MetricsRegistry.class), new CheckpointSerde());
    checkpointManager.register(TASK1);

    // 1. verify that consumer.register is called only during checkpointManager.start.
    // 2. verify that consumer.register is called with the oldest offset.
    // 3. verify that no other operation on the CheckpointManager re-invokes register since start offsets are set during
    // register
    verify(mockConsumer, times(0)).register(CHECKPOINT_SSP, oldestOffset);
    checkpointManager.start();
    verify(mockConsumer, times(1)).register(CHECKPOINT_SSP, oldestOffset);

    checkpointManager.readLastCheckpoint(TASK1);
    verify(mockConsumer, times(1)).register(CHECKPOINT_SSP, oldestOffset);
  }

  @Test
  public void testAllMessagesInTheLogAreRead() throws Exception {
    KafkaStreamSpec checkpointSpec = new KafkaStreamSpec(CHECKPOINT_TOPIC, CHECKPOINT_TOPIC,
        CHECKPOINT_SYSTEM, 1);
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(JobConfig.SSP_GROUPER_FACTORY())).thenReturn(GROUPER_FACTORY_CLASS);

    SystemStreamPartition ssp = new SystemStreamPartition("system-1", "input-topic", new Partition(0));

    int oldestOffset = 0;
    int newestOffset = 10;

    // mock out a consumer that returns ten checkpoint IMEs for the same ssp
    List<List<IncomingMessageEnvelope>> pollOutputs = new ArrayList<>();
    for(int offset = oldestOffset; offset <= newestOffset; offset++) {
      pollOutputs.add(ImmutableList.of(newCheckpointEnvelope(TASK1, ssp, Integer.toString(offset))));
    }

    // return one message at a time from each poll simulating a KafkaConsumer with max.poll.records = 1
    SystemConsumer mockConsumer = newConsumer(pollOutputs);
    SystemAdmin mockAdmin = newAdmin(Integer.toString(oldestOffset), Integer.toString(newestOffset));
    SystemFactory factory = newFactory(mock(SystemProducer.class), mockConsumer, mockAdmin);

    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(checkpointSpec, factory,
        true, mockConfig, mock(MetricsRegistry.class), new CheckpointSerde());
    checkpointManager.register(TASK1);
    checkpointManager.start();

    // check that all ten messages are read, and the checkpoint is the newest message
    Checkpoint checkpoint = checkpointManager.readLastCheckpoint(TASK1);
    Assert.assertEquals(checkpoint.getOffsets(), ImmutableMap.of(ssp, Integer.toString(newestOffset)));
  }

  /**
   * Create a new {@link SystemConsumer} that returns a list of messages sequentially at each subsequent poll.
   *
   * @param pollOutputs a list of poll outputs to be returned at subsequent polls.
   *                    The i'th call to consumer.poll() will return the list at pollOutputs[i]
   * @return the consumer
   */
  private SystemConsumer newConsumer(List<List<IncomingMessageEnvelope>> pollOutputs) throws Exception {
    SystemConsumer mockConsumer = mock(SystemConsumer.class);
    OngoingStubbing<Map> when = when(mockConsumer.poll(anySet(), anyLong()));
    for (List<IncomingMessageEnvelope> pollOutput : pollOutputs) {
      when = when.thenReturn(ImmutableMap.of(CHECKPOINT_SSP, pollOutput));
    }
    when.thenReturn(ImmutableMap.of());
    return mockConsumer;
  }

  /**
   * Create a new {@link SystemAdmin} that returns the provided oldest and newest offsets for its topics
   */
  private SystemAdmin newAdmin(String oldestOffset, String newestOffset) {
    SystemStreamMetadata checkpointTopicMetadata = new SystemStreamMetadata(CHECKPOINT_TOPIC,
        ImmutableMap.of(new Partition(0), new SystemStreamPartitionMetadata(oldestOffset,
            newestOffset, Integer.toString(Integer.parseInt(newestOffset) + 1))));
    SystemAdmin mockAdmin = mock(SystemAdmin.class);
    when(mockAdmin.getSystemStreamMetadata(Collections.singleton(CHECKPOINT_TOPIC))).thenReturn(
        ImmutableMap.of(CHECKPOINT_TOPIC, checkpointTopicMetadata));
    return mockAdmin;
  }

  private SystemFactory newFactory(SystemProducer producer, SystemConsumer consumer, SystemAdmin admin) {
    SystemFactory factory = mock(SystemFactory.class);
    when(factory.getProducer(anyString(), any(Config.class), any(MetricsRegistry.class))).thenReturn(producer);
    when(factory.getConsumer(anyString(), any(Config.class), any(MetricsRegistry.class))).thenReturn(consumer);
    when(factory.getAdmin(anyString(), any(Config.class))).thenReturn(admin);
    return factory;
  }

  /**
   * Creates a new checkpoint envelope for the provided task, ssp and offset
   */
  private IncomingMessageEnvelope newCheckpointEnvelope(TaskName taskName, SystemStreamPartition ssp, String offset) {
    KafkaCheckpointLogKey checkpointKey =
        new KafkaCheckpointLogKey(GROUPER_FACTORY_CLASS, taskName, "checkpoint");
    KafkaCheckpointLogKeySerde checkpointKeySerde = new KafkaCheckpointLogKeySerde();

    Checkpoint checkpointMsg = new Checkpoint(ImmutableMap.of(ssp, offset));
    CheckpointSerde checkpointMsgSerde = new CheckpointSerde();

    return new IncomingMessageEnvelope(CHECKPOINT_SSP, offset, checkpointKeySerde.toBytes(checkpointKey),
        checkpointMsgSerde.toBytes(checkpointMsg));
  }

  private static class ExceptionThrowingCheckpointSerde extends CheckpointSerde {
    public Checkpoint fromBytes(byte[] bytes) {
      throw new KafkaException("exception");
    }
  }
}
