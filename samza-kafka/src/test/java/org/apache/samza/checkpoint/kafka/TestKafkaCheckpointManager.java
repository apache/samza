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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV1;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.CheckpointV1Serde;
import org.apache.samza.serializers.CheckpointV2Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.kafka.KafkaStreamSpec;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public class TestKafkaCheckpointManager {
  private static final TaskName TASK0 = new TaskName("Partition 0");
  private static final TaskName TASK1 = new TaskName("Partition 1");
  private static final String CHECKPOINT_TOPIC = "checkpointTopic";
  private static final String CHECKPOINT_SYSTEM = "checkpointSystem";
  private static final SystemStreamPartition CHECKPOINT_SSP =
      new SystemStreamPartition(CHECKPOINT_SYSTEM, CHECKPOINT_TOPIC, new Partition(0));
  private static final SystemStreamPartition INPUT_SSP0 =
      new SystemStreamPartition("inputSystem", "inputTopic", new Partition(0));
  private static final SystemStreamPartition INPUT_SSP1 =
      new SystemStreamPartition("inputSystem", "inputTopic", new Partition(1));
  private static final String GROUPER_FACTORY_CLASS = GroupByPartitionFactory.class.getCanonicalName();
  private static final KafkaStreamSpec CHECKPOINT_SPEC =
      new KafkaStreamSpec(CHECKPOINT_TOPIC, CHECKPOINT_TOPIC, CHECKPOINT_SYSTEM, 1);
  private static final CheckpointV1Serde CHECKPOINT_V1_SERDE = new CheckpointV1Serde();
  private static final CheckpointV2Serde CHECKPOINT_V2_SERDE = new CheckpointV2Serde();
  private static final KafkaCheckpointLogKeySerde KAFKA_CHECKPOINT_LOG_KEY_SERDE = new KafkaCheckpointLogKeySerde();

  @Mock
  private SystemProducer systemProducer;
  @Mock
  private SystemConsumer systemConsumer;
  @Mock
  private SystemAdmin systemAdmin;
  @Mock
  private SystemAdmin createResourcesSystemAdmin;
  @Mock
  private SystemFactory systemFactory;
  @Mock
  private MetricsRegistry metricsRegistry;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test(expected = TopicAlreadyMarkedForDeletionException.class)
  public void testCreateResourcesTopicCreationError() {
    setupSystemFactory(config());
    // throw an exception during createStream
    doThrow(new TopicAlreadyMarkedForDeletionException("invalid stream")).when(this.createResourcesSystemAdmin)
        .createStream(CHECKPOINT_SPEC);
    KafkaCheckpointManager checkpointManager = buildKafkaCheckpointManager(true, config());
    // expect an exception during startup
    checkpointManager.createResources();
  }

  @Test(expected = StreamValidationException.class)
  public void testCreateResourcesTopicValidationError() {
    setupSystemFactory(config());
    // throw an exception during validateStream
    doThrow(new StreamValidationException("invalid stream")).when(this.createResourcesSystemAdmin)
        .validateStream(CHECKPOINT_SPEC);
    KafkaCheckpointManager checkpointManager = buildKafkaCheckpointManager(true, config());
    // expect an exception during startup
    checkpointManager.createResources();
  }

  @Test(expected = SamzaException.class)
  public void testReadFailsOnSerdeExceptions() throws InterruptedException {
    setupSystemFactory(config());
    List<IncomingMessageEnvelope> checkpointEnvelopes =
        ImmutableList.of(newCheckpointV1Envelope(TASK0, buildCheckpointV1(INPUT_SSP0, "0"), "0"));
    setupConsumer(checkpointEnvelopes);
    // wire up an exception throwing serde with the checkpointManager
    CheckpointV1Serde checkpointV1Serde = mock(CheckpointV1Serde.class);
    doThrow(new RuntimeException("serde failed")).when(checkpointV1Serde).fromBytes(any());
    KafkaCheckpointManager checkpointManager =
        new KafkaCheckpointManager(CHECKPOINT_SPEC, this.systemFactory, true, config(), this.metricsRegistry,
            checkpointV1Serde, CHECKPOINT_V2_SERDE, KAFKA_CHECKPOINT_LOG_KEY_SERDE);
    checkpointManager.register(TASK0);

    // expect an exception
    checkpointManager.readLastCheckpoint(TASK0);
  }

  @Test
  public void testReadSucceedsOnKeySerdeExceptionsWhenValidationIsDisabled() throws InterruptedException {
    setupSystemFactory(config());
    List<IncomingMessageEnvelope> checkpointEnvelopes =
        ImmutableList.of(newCheckpointV1Envelope(TASK0, buildCheckpointV1(INPUT_SSP0, "0"), "0"));
    setupConsumer(checkpointEnvelopes);
    // wire up an exception throwing serde with the checkpointManager
    CheckpointV1Serde checkpointV1Serde = mock(CheckpointV1Serde.class);
    doThrow(new RuntimeException("serde failed")).when(checkpointV1Serde).fromBytes(any());
    KafkaCheckpointManager checkpointManager =
        new KafkaCheckpointManager(CHECKPOINT_SPEC, this.systemFactory, false, config(), this.metricsRegistry,
            checkpointV1Serde, CHECKPOINT_V2_SERDE, KAFKA_CHECKPOINT_LOG_KEY_SERDE);
    checkpointManager.register(TASK0);

    // expect the read to succeed in spite of the exception from ExceptionThrowingSerde
    assertNull(checkpointManager.readLastCheckpoint(TASK0));
  }

  @Test
  public void testStart() {
    setupSystemFactory(config());
    String oldestOffset = "1";
    String newestOffset = "2";
    SystemStreamMetadata checkpointTopicMetadata = new SystemStreamMetadata(CHECKPOINT_TOPIC,
        ImmutableMap.of(new Partition(0), new SystemStreamPartitionMetadata(oldestOffset, newestOffset,
            Integer.toString(Integer.parseInt(newestOffset) + 1))));
    when(this.systemAdmin.getSystemStreamMetadata(Collections.singleton(CHECKPOINT_TOPIC))).thenReturn(
        ImmutableMap.of(CHECKPOINT_TOPIC, checkpointTopicMetadata));

    KafkaCheckpointManager checkpointManager = buildKafkaCheckpointManager(true, config());

    checkpointManager.start();

    verify(this.systemProducer).start();
    verify(this.systemAdmin).start();
    verify(this.systemConsumer).register(CHECKPOINT_SSP, oldestOffset);
    verify(this.systemConsumer).start();
  }

  @Test
  public void testRegister() {
    setupSystemFactory(config());
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.register(TASK0);
    verify(this.systemProducer).register(TASK0.getTaskName());
  }

  @Test
  public void testStop() {
    setupSystemFactory(config());
    KafkaCheckpointManager checkpointManager = buildKafkaCheckpointManager(true, config());
    checkpointManager.stop();
    verify(this.systemProducer).stop();
    // default configuration for stopConsumerAfterFirstRead means that consumer is not stopped here
    verify(this.systemConsumer, never()).stop();
    verify(this.systemAdmin).stop();
  }

  @Test
  public void testWriteCheckpointShouldRecreateSystemProducerOnFailure() {
    setupSystemFactory(config());
    SystemProducer secondKafkaProducer = mock(SystemProducer.class);
    // override default mock behavior to return a second producer on the second call to create a producer
    when(this.systemFactory.getProducer(CHECKPOINT_SYSTEM, config(), this.metricsRegistry,
        KafkaCheckpointManager.class.getSimpleName())).thenReturn(this.systemProducer, secondKafkaProducer);
    // first producer throws an exception on flush
    doThrow(new RuntimeException("flush failed")).when(this.systemProducer).flush(TASK0.getTaskName());
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.register(TASK0);

    CheckpointV1 checkpointV1 = buildCheckpointV1(INPUT_SSP0, "0");
    kafkaCheckpointManager.writeCheckpoint(TASK0, checkpointV1);

    // first producer should be stopped
    verify(this.systemProducer).stop();
    // register and start the second producer
    verify(secondKafkaProducer).register(TASK0.getTaskName());
    verify(secondKafkaProducer).start();
    // check that the second producer was given the message to send out
    ArgumentCaptor<OutgoingMessageEnvelope> outgoingMessageEnvelopeArgumentCaptor =
        ArgumentCaptor.forClass(OutgoingMessageEnvelope.class);
    verify(secondKafkaProducer).send(eq(TASK0.getTaskName()), outgoingMessageEnvelopeArgumentCaptor.capture());
    assertEquals(CHECKPOINT_SSP, outgoingMessageEnvelopeArgumentCaptor.getValue().getSystemStream());
    assertEquals(new KafkaCheckpointLogKey(KafkaCheckpointLogKey.CHECKPOINT_V1_KEY_TYPE, TASK0, GROUPER_FACTORY_CLASS),
        KAFKA_CHECKPOINT_LOG_KEY_SERDE.fromBytes((byte[]) outgoingMessageEnvelopeArgumentCaptor.getValue().getKey()));
    assertEquals(checkpointV1,
        CHECKPOINT_V1_SERDE.fromBytes((byte[]) outgoingMessageEnvelopeArgumentCaptor.getValue().getMessage()));
    verify(secondKafkaProducer).flush(TASK0.getTaskName());
  }

  @Test
  public void testCreateResources() {
    setupSystemFactory(config());
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.createResources();

    verify(this.createResourcesSystemAdmin).start();
    verify(this.createResourcesSystemAdmin).createStream(CHECKPOINT_SPEC);
    verify(this.createResourcesSystemAdmin).validateStream(CHECKPOINT_SPEC);
    verify(this.createResourcesSystemAdmin).stop();
  }

  @Test
  public void testCreateResourcesSkipValidation() {
    setupSystemFactory(config());
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(false, config());
    kafkaCheckpointManager.createResources();

    verify(this.createResourcesSystemAdmin).start();
    verify(this.createResourcesSystemAdmin).createStream(CHECKPOINT_SPEC);
    verify(this.createResourcesSystemAdmin, never()).validateStream(CHECKPOINT_SPEC);
    verify(this.createResourcesSystemAdmin).stop();
  }

  @Test
  public void testReadEmpty() throws InterruptedException {
    setupSystemFactory(config());
    setupConsumer(ImmutableList.of());
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.register(TASK0);
    assertNull(kafkaCheckpointManager.readLastCheckpoint(TASK0));
  }

  @Test
  public void testReadCheckpointV1() throws InterruptedException {
    setupSystemFactory(config());
    CheckpointV1 checkpointV1 = buildCheckpointV1(INPUT_SSP0, "0");
    List<IncomingMessageEnvelope> checkpointEnvelopes =
        ImmutableList.of(newCheckpointV1Envelope(TASK0, checkpointV1, "0"));
    setupConsumer(checkpointEnvelopes);
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.register(TASK0);
    Checkpoint actualCheckpoint = kafkaCheckpointManager.readLastCheckpoint(TASK0);
    assertEquals(checkpointV1, actualCheckpoint);
  }

  @Test
  public void testReadIgnoreCheckpointV2WhenV1Enabled() throws InterruptedException {
    setupSystemFactory(config());
    CheckpointV1 checkpointV1 = buildCheckpointV1(INPUT_SSP0, "0");
    List<IncomingMessageEnvelope> checkpointEnvelopes =
        ImmutableList.of(newCheckpointV1Envelope(TASK0, checkpointV1, "0"),
            newCheckpointV2Envelope(TASK0, buildCheckpointV2(INPUT_SSP0, "1"), "1"));
    setupConsumer(checkpointEnvelopes);
    // default is to only read CheckpointV1
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.register(TASK0);
    Checkpoint actualCheckpoint = kafkaCheckpointManager.readLastCheckpoint(TASK0);
    assertEquals(checkpointV1, actualCheckpoint);
  }

  @Test
  public void testReadCheckpointV2() throws InterruptedException {
    Config config = config(ImmutableMap.of(TaskConfig.CHECKPOINT_READ_VERSIONS, "1,2"));
    setupSystemFactory(config);
    CheckpointV2 checkpointV2 = buildCheckpointV2(INPUT_SSP0, "0");
    List<IncomingMessageEnvelope> checkpointEnvelopes =
        ImmutableList.of(newCheckpointV2Envelope(TASK0, checkpointV2, "0"));
    setupConsumer(checkpointEnvelopes);
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config);
    kafkaCheckpointManager.register(TASK0);
    Checkpoint actualCheckpoint = kafkaCheckpointManager.readLastCheckpoint(TASK0);
    assertEquals(checkpointV2, actualCheckpoint);
  }

  @Test
  public void testReadCheckpointPriority() throws InterruptedException {
    Config config = config(ImmutableMap.of(TaskConfig.CHECKPOINT_READ_VERSIONS, "2,1"));
    setupSystemFactory(config);
    CheckpointV2 checkpointV2 = buildCheckpointV2(INPUT_SSP0, "1");
    List<IncomingMessageEnvelope> checkpointEnvelopes =
        ImmutableList.of(newCheckpointV1Envelope(TASK0, buildCheckpointV1(INPUT_SSP0, "0"), "0"),
            newCheckpointV2Envelope(TASK0, checkpointV2, "1"));
    setupConsumer(checkpointEnvelopes);
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config);
    kafkaCheckpointManager.register(TASK0);
    Checkpoint actualCheckpoint = kafkaCheckpointManager.readLastCheckpoint(TASK0);
    assertEquals(checkpointV2, actualCheckpoint);
  }

  @Test
  public void testReadMultipleCheckpointsMultipleSSP() throws InterruptedException {
    setupSystemFactory(config());
    KafkaCheckpointManager checkpointManager = buildKafkaCheckpointManager(true, config());
    checkpointManager.register(TASK0);
    checkpointManager.register(TASK1);

    // mock out a consumer that returns 5 checkpoint IMEs for each SSP
    int newestOffset = 5;
    int checkpointOffsetCounter = 0;
    List<List<IncomingMessageEnvelope>> pollOutputs = new ArrayList<>();
    for (int offset = 1; offset <= newestOffset; offset++) {
      pollOutputs.add(ImmutableList.of(
          // use regular offset value for INPUT_SSP0
          newCheckpointV1Envelope(TASK0, buildCheckpointV1(INPUT_SSP0, Integer.toString(offset)),
              Integer.toString(checkpointOffsetCounter++)),
          // use (offset * 2) value for INPUT_SSP1 so offsets are different from INPUT_SSP0
          newCheckpointV1Envelope(TASK1, buildCheckpointV1(INPUT_SSP1, Integer.toString(offset * 2)),
              Integer.toString(checkpointOffsetCounter++))));
    }
    setupConsumerMultiplePoll(pollOutputs);

    assertEquals(buildCheckpointV1(INPUT_SSP0, Integer.toString(newestOffset)),
        checkpointManager.readLastCheckpoint(TASK0));
    assertEquals(buildCheckpointV1(INPUT_SSP1, Integer.toString(newestOffset * 2)),
        checkpointManager.readLastCheckpoint(TASK1));
    // check expected number of polls (+1 is for the final empty poll), and the checkpoint is the newest message
    verify(this.systemConsumer, times(newestOffset + 1)).poll(ImmutableSet.of(CHECKPOINT_SSP),
        SystemConsumer.BLOCK_ON_OUTSTANDING_MESSAGES);
  }

  @Test
  public void testReadMultipleCheckpointsUpgradeCheckpointVersion() throws InterruptedException {
    Config config = config(ImmutableMap.of(TaskConfig.CHECKPOINT_READ_VERSIONS, "2,1"));
    setupSystemFactory(config);
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config);
    kafkaCheckpointManager.register(TASK0);
    kafkaCheckpointManager.register(TASK1);

    List<IncomingMessageEnvelope> checkpointEnvelopesV1 =
        ImmutableList.of(newCheckpointV1Envelope(TASK0, buildCheckpointV1(INPUT_SSP0, "0"), "0"),
            newCheckpointV1Envelope(TASK1, buildCheckpointV1(INPUT_SSP1, "0"), "1"));
    CheckpointV2 ssp0CheckpointV2 = buildCheckpointV2(INPUT_SSP0, "10");
    CheckpointV2 ssp1CheckpointV2 = buildCheckpointV2(INPUT_SSP1, "11");
    List<IncomingMessageEnvelope> checkpointEnvelopesV2 =
        ImmutableList.of(newCheckpointV2Envelope(TASK0, ssp0CheckpointV2, "2"),
            newCheckpointV2Envelope(TASK1, ssp1CheckpointV2, "3"));
    setupConsumerMultiplePoll(ImmutableList.of(checkpointEnvelopesV1, checkpointEnvelopesV2));
    assertEquals(ssp0CheckpointV2, kafkaCheckpointManager.readLastCheckpoint(TASK0));
    assertEquals(ssp1CheckpointV2, kafkaCheckpointManager.readLastCheckpoint(TASK1));
    // 2 polls for actual checkpoints, 1 final empty poll
    verify(this.systemConsumer, times(3)).poll(ImmutableSet.of(CHECKPOINT_SSP),
        SystemConsumer.BLOCK_ON_OUTSTANDING_MESSAGES);
  }

  @Test
  public void testReadAllCheckpoints() throws InterruptedException {
    Config config = config(ImmutableMap.of(TaskConfig.CHECKPOINT_READ_VERSIONS, "1,2"));
    setupSystemFactory(config);
    CheckpointV2 checkpointV2ForTask0 = buildCheckpointV2(INPUT_SSP0, "0");
    CheckpointV2 checkpointV2ForTask1 = buildCheckpointV2(INPUT_SSP0, "1");
    List<IncomingMessageEnvelope> checkpointEnvelopes =
        ImmutableList.of(
            newCheckpointV2Envelope(TASK0, checkpointV2ForTask0, "0"),
            newCheckpointV2Envelope(TASK1, checkpointV2ForTask1, "1")
            );
    setupConsumer(checkpointEnvelopes);
    Map<TaskName, Checkpoint> checkpointMap = ImmutableMap.of(TASK0, checkpointV2ForTask0, TASK1, checkpointV2ForTask1);
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config);
    kafkaCheckpointManager.register(TASK0);
    Map<TaskName, Checkpoint> readCheckpoints = kafkaCheckpointManager.readAllCheckpoints();
    assertEquals(checkpointMap, readCheckpoints);
  }

  @Test
  public void testWriteCheckpointV1() {
    setupSystemFactory(config());
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.register(TASK0);
    CheckpointV1 checkpointV1 = buildCheckpointV1(INPUT_SSP0, "0");
    kafkaCheckpointManager.writeCheckpoint(TASK0, checkpointV1);
    ArgumentCaptor<OutgoingMessageEnvelope> outgoingMessageEnvelopeArgumentCaptor =
        ArgumentCaptor.forClass(OutgoingMessageEnvelope.class);
    verify(this.systemProducer).send(eq(TASK0.getTaskName()), outgoingMessageEnvelopeArgumentCaptor.capture());
    assertEquals(CHECKPOINT_SSP, outgoingMessageEnvelopeArgumentCaptor.getValue().getSystemStream());
    assertEquals(new KafkaCheckpointLogKey(KafkaCheckpointLogKey.CHECKPOINT_V1_KEY_TYPE, TASK0, GROUPER_FACTORY_CLASS),
        KAFKA_CHECKPOINT_LOG_KEY_SERDE.fromBytes((byte[]) outgoingMessageEnvelopeArgumentCaptor.getValue().getKey()));
    assertEquals(checkpointV1,
        CHECKPOINT_V1_SERDE.fromBytes((byte[]) outgoingMessageEnvelopeArgumentCaptor.getValue().getMessage()));
    verify(this.systemProducer).flush(TASK0.getTaskName());
  }

  @Test
  public void testWriteCheckpointV2() {
    setupSystemFactory(config());
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.register(TASK0);
    CheckpointV2 checkpointV2 = buildCheckpointV2(INPUT_SSP0, "0");
    kafkaCheckpointManager.writeCheckpoint(TASK0, checkpointV2);
    ArgumentCaptor<OutgoingMessageEnvelope> outgoingMessageEnvelopeArgumentCaptor =
        ArgumentCaptor.forClass(OutgoingMessageEnvelope.class);
    verify(this.systemProducer).send(eq(TASK0.getTaskName()), outgoingMessageEnvelopeArgumentCaptor.capture());
    assertEquals(CHECKPOINT_SSP, outgoingMessageEnvelopeArgumentCaptor.getValue().getSystemStream());
    assertEquals(new KafkaCheckpointLogKey(KafkaCheckpointLogKey.CHECKPOINT_V2_KEY_TYPE, TASK0, GROUPER_FACTORY_CLASS),
        KAFKA_CHECKPOINT_LOG_KEY_SERDE.fromBytes((byte[]) outgoingMessageEnvelopeArgumentCaptor.getValue().getKey()));
    assertEquals(checkpointV2,
        CHECKPOINT_V2_SERDE.fromBytes((byte[]) outgoingMessageEnvelopeArgumentCaptor.getValue().getMessage()));
    verify(this.systemProducer).flush(TASK0.getTaskName());
  }

  @Test
  public void testWriteCheckpointShouldRetryFiniteTimesOnFailure() {
    setupSystemFactory(config());
    doThrow(new RuntimeException("send failed")).when(this.systemProducer).send(any(), any());
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.register(TASK0);
    kafkaCheckpointManager.MaxRetryDurationInMillis_$eq(100); // setter for scala var MaxRetryDurationInMillis
    CheckpointV2 checkpointV2 = buildCheckpointV2(INPUT_SSP0, "0");
    try {
      kafkaCheckpointManager.writeCheckpoint(TASK0, checkpointV2);
      fail("Expected to throw SamzaException");
    } catch (SamzaException e) {
      // expected to get here
    }
    // one call to send which fails, then writeCheckpoint gives up
    verify(this.systemProducer).send(any(), any());
    verify(this.systemProducer, never()).flush(any());
  }

  @Test
  public void testConsumerStopsAfterInitialRead() throws Exception {
    setupSystemFactory(config());
    CheckpointV1 checkpointV1 = buildCheckpointV1(INPUT_SSP0, "0");
    setupConsumer(ImmutableList.of(newCheckpointV1Envelope(TASK0, checkpointV1, "0")));
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config());
    kafkaCheckpointManager.register(TASK0);
    assertEquals(checkpointV1, kafkaCheckpointManager.readLastCheckpoint(TASK0));
    // 1 call to get actual checkpoints, 1 call for empty poll to signal done reading
    verify(this.systemConsumer, times(2)).poll(ImmutableSet.of(CHECKPOINT_SSP), SystemConsumer.BLOCK_ON_OUTSTANDING_MESSAGES);
    verify(this.systemConsumer).stop();
    // reading checkpoint again should not read more messages from the consumer
    assertEquals(checkpointV1, kafkaCheckpointManager.readLastCheckpoint(TASK0));
    verifyNoMoreInteractions(this.systemConsumer);
  }

  @Test
  public void testConsumerStopsAfterInitialReadDisabled() throws Exception {
    Config config =
        config(ImmutableMap.of(TaskConfig.INTERNAL_CHECKPOINT_MANAGER_CONSUMER_STOP_AFTER_FIRST_READ, "false"));
    setupSystemFactory(config);
    // 1) return checkpointV1 for INPUT_SSP
    CheckpointV1 ssp0FirstCheckpointV1 = buildCheckpointV1(INPUT_SSP0, "0");
    List<IncomingMessageEnvelope> checkpointEnvelopes0 =
        ImmutableList.of(newCheckpointV1Envelope(TASK0, buildCheckpointV1(INPUT_SSP0, "0"), "0"));
    setupConsumer(checkpointEnvelopes0);
    KafkaCheckpointManager kafkaCheckpointManager = buildKafkaCheckpointManager(true, config);
    kafkaCheckpointManager.register(TASK0);
    assertEquals(ssp0FirstCheckpointV1, kafkaCheckpointManager.readLastCheckpoint(TASK0));

    // 2) return new checkpointV1 for just INPUT_SSP
    CheckpointV1 ssp0SecondCheckpointV1 = buildCheckpointV1(INPUT_SSP0, "10");
    List<IncomingMessageEnvelope> checkpointEnvelopes1 =
        ImmutableList.of(newCheckpointV1Envelope(TASK0, ssp0SecondCheckpointV1, "1"));
    setupConsumer(checkpointEnvelopes1);
    assertEquals(ssp0SecondCheckpointV1, kafkaCheckpointManager.readLastCheckpoint(TASK0));

    verify(this.systemConsumer, never()).stop();
  }

  private KafkaCheckpointManager buildKafkaCheckpointManager(boolean validateCheckpoint, Config config) {
    return new KafkaCheckpointManager(CHECKPOINT_SPEC, this.systemFactory, validateCheckpoint, config,
        this.metricsRegistry, CHECKPOINT_V1_SERDE, CHECKPOINT_V2_SERDE, KAFKA_CHECKPOINT_LOG_KEY_SERDE);
  }

  private void setupConsumer(List<IncomingMessageEnvelope> pollOutput) throws InterruptedException {
    setupConsumerMultiplePoll(ImmutableList.of(pollOutput));
  }

  /**
   * Create a new {@link SystemConsumer} that returns a list of messages sequentially at each subsequent poll.
   *
   * @param pollOutputs a list of poll outputs to be returned at subsequent polls.
   *                    The i'th call to consumer.poll() will return the list at pollOutputs[i]
   */
  private void setupConsumerMultiplePoll(List<List<IncomingMessageEnvelope>> pollOutputs) throws InterruptedException {
    OngoingStubbing<Map<SystemStreamPartition, List<IncomingMessageEnvelope>>> when =
        when(this.systemConsumer.poll(ImmutableSet.of(CHECKPOINT_SSP), SystemConsumer.BLOCK_ON_OUTSTANDING_MESSAGES));
    for (List<IncomingMessageEnvelope> pollOutput : pollOutputs) {
      when = when.thenReturn(ImmutableMap.of(CHECKPOINT_SSP, pollOutput));
    }
    when.thenReturn(ImmutableMap.of());
  }

  private void setupSystemFactory(Config config) {
    when(this.systemFactory.getProducer(CHECKPOINT_SYSTEM, config, this.metricsRegistry,
        KafkaCheckpointManager.class.getSimpleName())).thenReturn(this.systemProducer);
    when(this.systemFactory.getConsumer(CHECKPOINT_SYSTEM, config, this.metricsRegistry,
        KafkaCheckpointManager.class.getSimpleName())).thenReturn(this.systemConsumer);
    when(this.systemFactory.getAdmin(CHECKPOINT_SYSTEM, config,
        KafkaCheckpointManager.class.getSimpleName())).thenReturn(this.systemAdmin);
    when(this.systemFactory.getAdmin(CHECKPOINT_SYSTEM, config,
        KafkaCheckpointManager.class.getSimpleName() + "createResource")).thenReturn(this.createResourcesSystemAdmin);
  }

  private static CheckpointV1 buildCheckpointV1(SystemStreamPartition ssp, String offset) {
    return new CheckpointV1(ImmutableMap.of(ssp, offset));
  }

  /**
   * Creates a new checkpoint envelope for the provided task, ssp and offset
   */
  private IncomingMessageEnvelope newCheckpointV1Envelope(TaskName taskName, CheckpointV1 checkpointV1,
      String checkpointMessageOffset) {
    KafkaCheckpointLogKey checkpointKey = new KafkaCheckpointLogKey("checkpoint", taskName, GROUPER_FACTORY_CLASS);
    KafkaCheckpointLogKeySerde checkpointKeySerde = new KafkaCheckpointLogKeySerde();
    CheckpointV1Serde checkpointMsgSerde = new CheckpointV1Serde();
    return new IncomingMessageEnvelope(CHECKPOINT_SSP, checkpointMessageOffset,
        checkpointKeySerde.toBytes(checkpointKey), checkpointMsgSerde.toBytes(checkpointV1));
  }

  private static CheckpointV2 buildCheckpointV2(SystemStreamPartition ssp, String offset) {
    return new CheckpointV2(CheckpointId.create(), ImmutableMap.of(ssp, offset),
        ImmutableMap.of("backend", ImmutableMap.of("store", "10")));
  }

  private IncomingMessageEnvelope newCheckpointV2Envelope(TaskName taskName, CheckpointV2 checkpointV2,
      String checkpointMessageOffset) {
    KafkaCheckpointLogKey checkpointKey =
        new KafkaCheckpointLogKey(KafkaCheckpointLogKey.CHECKPOINT_V2_KEY_TYPE, taskName, GROUPER_FACTORY_CLASS);
    KafkaCheckpointLogKeySerde checkpointKeySerde = new KafkaCheckpointLogKeySerde();
    CheckpointV2Serde checkpointMsgSerde = new CheckpointV2Serde();
    return new IncomingMessageEnvelope(CHECKPOINT_SSP, checkpointMessageOffset,
        checkpointKeySerde.toBytes(checkpointKey), checkpointMsgSerde.toBytes(checkpointV2));
  }

  /**
   * Build base {@link Config} for tests.
   */
  private static Config config() {
    return new MapConfig(ImmutableMap.of(JobConfig.SSP_GROUPER_FACTORY, GROUPER_FACTORY_CLASS));
  }

  private static Config config(Map<String, String> additional) {
    Map<String, String> configMap = new HashMap<>(config());
    configMap.putAll(additional);
    return new MapConfig(configMap);
  }
}