/*
 *
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
 *
 */

package org.apache.samza.system.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.KafkaConsumerConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.startpoint.StartpointOldest;
import org.apache.samza.startpoint.StartpointSpecific;
import org.apache.samza.startpoint.StartpointTimestamp;
import org.apache.samza.startpoint.StartpointUpcoming;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.kafka.KafkaSystemConsumer.KafkaStartpointRegistrationHandler;
import org.apache.samza.testUtils.TestClock;
import org.apache.samza.util.Clock;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class TestKafkaSystemConsumer {
  private static final String TEST_SYSTEM = "test-system";
  private static final String TEST_STREAM = "test-stream";
  private static final String TEST_JOB = "test-job";
  private static final String TEST_CLIENT_ID = "testClientId";
  private static final String BOOTSTRAP_SERVER = "127.0.0.1:8888";
  private static final String FETCH_THRESHOLD_MSGS = "50000";
  private static final String FETCH_THRESHOLD_BYTES = "100000";

  private static final Integer TEST_PARTITION_ID = 0;
  private static final TopicPartition TEST_TOPIC_PARTITION = new TopicPartition(TEST_STREAM, TEST_PARTITION_ID);
  private static final Partition TEST_PARTITION = new Partition(TEST_PARTITION_ID);
  private static final SystemStreamPartition TEST_SYSTEM_STREAM_PARTITION = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, TEST_PARTITION);
  private static final String TEST_OFFSET = "10";

  private KafkaSystemConsumer createConsumer(String fetchMsg, String fetchBytes) {
    final Map<String, String> map = new HashMap<>();

    map.put(JobConfig.JOB_NAME(), TEST_JOB);

    map.put(String.format(KafkaConfig.CONSUMER_FETCH_THRESHOLD(), TEST_SYSTEM), fetchMsg);
    map.put(String.format(KafkaConfig.CONSUMER_FETCH_THRESHOLD_BYTES(), TEST_SYSTEM), fetchBytes);
    map.put(String.format("systems.%s.consumer.%s", TEST_SYSTEM, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
        BOOTSTRAP_SERVER);
    map.put(JobConfig.JOB_NAME(), "jobName");

    Config config = new MapConfig(map);
    String clientId = KafkaConsumerConfig.createClientId(TEST_CLIENT_ID, config);
    KafkaConsumerConfig consumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, TEST_SYSTEM, clientId);

    final KafkaConsumer<byte[], byte[]> kafkaConsumer = new MockKafkaConsumer(consumerConfig);

    MockKafkaSystemConsumer newKafkaSystemConsumer =
        new MockKafkaSystemConsumer(kafkaConsumer, TEST_SYSTEM, config, TEST_CLIENT_ID,
            new KafkaSystemConsumerMetrics(TEST_SYSTEM, new NoOpMetricsRegistry()), System::currentTimeMillis);

    return newKafkaSystemConsumer;
  }

  @Test
  public void testConfigValidations() {

    final KafkaSystemConsumer consumer = createConsumer(FETCH_THRESHOLD_MSGS, FETCH_THRESHOLD_BYTES);

    consumer.start();
    // should be no failures
  }

  @Test
  public void testFetchThresholdShouldDivideEvenlyAmongPartitions() {
    final KafkaSystemConsumer consumer = createConsumer(FETCH_THRESHOLD_MSGS, FETCH_THRESHOLD_BYTES);
    final int partitionsNum = 50;
    for (int i = 0; i < partitionsNum; i++) {
      consumer.register(new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(i)), "0");
    }

    consumer.start();

    Assert.assertEquals(Long.valueOf(FETCH_THRESHOLD_MSGS) / partitionsNum, consumer.perPartitionFetchThreshold);
    Assert.assertEquals(Long.valueOf(FETCH_THRESHOLD_BYTES) / 2 / partitionsNum,
        consumer.perPartitionFetchThresholdBytes);

    consumer.stop();
  }

  @Test
  public void testConsumerShouldRegisterTheLatestOffsetForSSP() {
    KafkaSystemConsumer consumer = createConsumer(FETCH_THRESHOLD_MSGS, FETCH_THRESHOLD_BYTES);

    SystemStreamPartition ssp0 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(0));
    SystemStreamPartition ssp1 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(1));
    SystemStreamPartition ssp2 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(2));

    consumer.register(ssp0, "0");
    consumer.register(ssp0, "5");
    consumer.register(ssp1, "2");
    consumer.register(ssp1, "3");
    consumer.register(ssp2, "0");

    consumer.start();

    assertEquals("5", ((StartpointSpecific) consumer.topicPartitionToStartpointMap.get(KafkaSystemConsumer.toTopicPartition(ssp0))).getSpecificOffset());
    assertEquals("3", ((StartpointSpecific) consumer.topicPartitionToStartpointMap.get(KafkaSystemConsumer.toTopicPartition(ssp1))).getSpecificOffset());
    assertEquals("0", ((StartpointSpecific) consumer.topicPartitionToStartpointMap.get(KafkaSystemConsumer.toTopicPartition(ssp2))).getSpecificOffset());
  }

  @Test
  public void testFetchThresholdBytes() {
    SystemStreamPartition ssp0 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(0));
    SystemStreamPartition ssp1 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(1));
    int partitionsNum = 2;
    int ime0Size = Integer.valueOf(FETCH_THRESHOLD_MSGS) / partitionsNum; // fake size
    int ime1Size = Integer.valueOf(FETCH_THRESHOLD_MSGS) / partitionsNum - 1; // fake size
    int ime11Size = 20;
    ByteArraySerializer bytesSerde = new ByteArraySerializer();
    IncomingMessageEnvelope ime0 = new IncomingMessageEnvelope(ssp0, "0", bytesSerde.serialize("", "key0".getBytes()),
        bytesSerde.serialize("", "value0".getBytes()), ime0Size);
    IncomingMessageEnvelope ime1 = new IncomingMessageEnvelope(ssp1, "0", bytesSerde.serialize("", "key1".getBytes()),
        bytesSerde.serialize("", "value1".getBytes()), ime1Size);
    IncomingMessageEnvelope ime11 = new IncomingMessageEnvelope(ssp1, "0", bytesSerde.serialize("", "key11".getBytes()),
        bytesSerde.serialize("", "value11".getBytes()), ime11Size);
    KafkaSystemConsumer consumer = createConsumer(FETCH_THRESHOLD_MSGS, FETCH_THRESHOLD_BYTES);

    consumer.register(ssp0, "0");
    consumer.register(ssp1, "0");
    consumer.start();
    consumer.messageSink.addMessage(ssp0, ime0);
    // queue for ssp0 should be full now, because we added message of size FETCH_THRESHOLD_MSGS/partitionsNum
    Assert.assertFalse(consumer.messageSink.needsMoreMessages(ssp0));
    consumer.messageSink.addMessage(ssp1, ime1);
    // queue for ssp1 should be less then full now, because we added message of size (FETCH_THRESHOLD_MSGS/partitionsNum - 1)
    Assert.assertTrue(consumer.messageSink.needsMoreMessages(ssp1));
    consumer.messageSink.addMessage(ssp1, ime11);
    // queue for ssp1 should full now, because we added message of size 20 on top
    Assert.assertFalse(consumer.messageSink.needsMoreMessages(ssp1));

    Assert.assertEquals(1, consumer.getNumMessagesInQueue(ssp0));
    Assert.assertEquals(2, consumer.getNumMessagesInQueue(ssp1));
    Assert.assertEquals(ime0Size, consumer.getMessagesSizeInQueue(ssp0));
    Assert.assertEquals(ime1Size + ime11Size, consumer.getMessagesSizeInQueue(ssp1));

    consumer.stop();
  }

  @Test
  public void testFetchThresholdBytesDiabled() {
    // Pass 0 as fetchThresholdByBytes, which disables checking for limit by size

    SystemStreamPartition ssp0 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(0));
    SystemStreamPartition ssp1 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(1));
    int partitionsNum = 2;
    int ime0Size = Integer.valueOf(FETCH_THRESHOLD_MSGS) / partitionsNum; // fake size, upto the limit
    int ime1Size = Integer.valueOf(FETCH_THRESHOLD_MSGS) / partitionsNum - 100; // fake size, below the limit
    int ime11Size = 20;// event with the second message still below the size limit
    ByteArraySerializer bytesSerde = new ByteArraySerializer();
    IncomingMessageEnvelope ime0 = new IncomingMessageEnvelope(ssp0, "0", bytesSerde.serialize("", "key0".getBytes()),
        bytesSerde.serialize("", "value0".getBytes()), ime0Size);
    IncomingMessageEnvelope ime1 = new IncomingMessageEnvelope(ssp1, "0", bytesSerde.serialize("", "key1".getBytes()),
        bytesSerde.serialize("", "value1".getBytes()), ime1Size);
    IncomingMessageEnvelope ime11 = new IncomingMessageEnvelope(ssp1, "0", bytesSerde.serialize("", "key11".getBytes()),
        bytesSerde.serialize("", "value11".getBytes()), ime11Size);

    // limit by number of messages 4/2 = 2 per partition
    // limit by number of bytes - disabled
    KafkaSystemConsumer consumer = createConsumer("4", "0"); // should disable

    consumer.register(ssp0, "0");
    consumer.register(ssp1, "0");
    consumer.start();
    consumer.messageSink.addMessage(ssp0, ime0);
    // should be full by size, but not full by number of messages (1 of 2)
    Assert.assertTrue(consumer.messageSink.needsMoreMessages(ssp0));
    consumer.messageSink.addMessage(ssp1, ime1);
    // not full neither by size nor by messages
    Assert.assertTrue(consumer.messageSink.needsMoreMessages(ssp1));
    consumer.messageSink.addMessage(ssp1, ime11);
    // not full by size, but should be full by messages
    Assert.assertFalse(consumer.messageSink.needsMoreMessages(ssp1));

    Assert.assertEquals(1, consumer.getNumMessagesInQueue(ssp0));
    Assert.assertEquals(2, consumer.getNumMessagesInQueue(ssp1));
    Assert.assertEquals(ime0Size, consumer.getMessagesSizeInQueue(ssp0));
    Assert.assertEquals(ime1Size + ime11Size, consumer.getMessagesSizeInQueue(ssp1));

    consumer.stop();
  }

  @Test
  public void testStartpointSpecificOffsetVisitorShouldUpdateTheFetchOffsetInConsumer() {
    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);
    final KafkaConsumerProxy kafkaConsumerProxy = Mockito.mock(KafkaConsumerProxy.class);
    final KafkaStartpointRegistrationHandler kafkaStartpointRegistrationHandler =  new KafkaStartpointRegistrationHandler(consumer, kafkaConsumerProxy);

    final StartpointSpecific testStartpointSpecific = new StartpointSpecific(TEST_OFFSET);

    // Mock the consumer interactions.
    Mockito.doNothing().when(consumer).seek(TEST_TOPIC_PARTITION, Long.valueOf(TEST_OFFSET));
    Mockito.when(consumer.position(TEST_TOPIC_PARTITION)).thenReturn(Long.valueOf(TEST_OFFSET));

    // Invoke the consumer with startpoint.
    kafkaStartpointRegistrationHandler.visit(TEST_SYSTEM_STREAM_PARTITION, testStartpointSpecific);

    // Mock verifications.
    Mockito.verify(consumer).seek(TEST_TOPIC_PARTITION, Long.valueOf(TEST_OFFSET));
    Mockito.verify(kafkaConsumerProxy).addTopicPartition(TEST_SYSTEM_STREAM_PARTITION, Long.valueOf(TEST_OFFSET));
  }

  @Test
  public void testStartpointTimestampVisitorShouldUpdateTheFetchOffsetInConsumer() {
    // Define dummy variables for testing.
    final Long testTimeStamp = 10L;

    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);
    final KafkaConsumerProxy kafkaConsumerProxy = Mockito.mock(KafkaConsumerProxy.class);
    final KafkaStartpointRegistrationHandler kafkaStartpointRegistrationHandler =  new KafkaStartpointRegistrationHandler(consumer, kafkaConsumerProxy);

    final StartpointTimestamp startpointTimestamp = new StartpointTimestamp(testTimeStamp);
    final Map<TopicPartition, OffsetAndTimestamp> offsetForTimesResult = ImmutableMap.of(
        TEST_TOPIC_PARTITION, new OffsetAndTimestamp(Long.valueOf(TEST_OFFSET), testTimeStamp));

    // Mock the consumer interactions.
    Mockito.when(consumer.offsetsForTimes(ImmutableMap.of(TEST_TOPIC_PARTITION, testTimeStamp))).thenReturn(offsetForTimesResult);
    Mockito.doNothing().when(consumer).seek(TEST_TOPIC_PARTITION, Long.valueOf(TEST_OFFSET));
    Mockito.when(consumer.position(TEST_TOPIC_PARTITION)).thenReturn(Long.valueOf(TEST_OFFSET));

    kafkaStartpointRegistrationHandler.visit(TEST_SYSTEM_STREAM_PARTITION, startpointTimestamp);

    // Mock verifications.
    Mockito.verify(consumer).seek(TEST_TOPIC_PARTITION, Long.valueOf(TEST_OFFSET));
    Mockito.verify(consumer).offsetsForTimes(ImmutableMap.of(TEST_TOPIC_PARTITION, testTimeStamp));
    Mockito.verify(kafkaConsumerProxy).addTopicPartition(TEST_SYSTEM_STREAM_PARTITION, Long.valueOf(TEST_OFFSET));
  }

  @Test
  public void testStartpointTimestampVisitorShouldMoveTheConsumerToEndWhenTimestampDoesNotExist() {
    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);
    final KafkaConsumerProxy kafkaConsumerProxy = Mockito.mock(KafkaConsumerProxy.class);
    final KafkaStartpointRegistrationHandler kafkaStartpointRegistrationHandler =  new KafkaStartpointRegistrationHandler(consumer, kafkaConsumerProxy);

    final StartpointTimestamp startpointTimestamp = new StartpointTimestamp(0L);
    final Map<TopicPartition, OffsetAndTimestamp> offsetForTimesResult = new HashMap<>();
    offsetForTimesResult.put(TEST_TOPIC_PARTITION, null);

    // Mock the consumer interactions.
    Mockito.when(consumer.offsetsForTimes(ImmutableMap.of(TEST_TOPIC_PARTITION, 0L))).thenReturn(offsetForTimesResult);
    Mockito.when(consumer.position(TEST_TOPIC_PARTITION)).thenReturn(Long.valueOf(TEST_OFFSET));

    kafkaStartpointRegistrationHandler.visit(TEST_SYSTEM_STREAM_PARTITION, startpointTimestamp);

    // Mock verifications.
    Mockito.verify(consumer).seekToEnd(ImmutableList.of(TEST_TOPIC_PARTITION));
    Mockito.verify(consumer).offsetsForTimes(ImmutableMap.of(TEST_TOPIC_PARTITION, 0L));
    Mockito.verify(kafkaConsumerProxy).addTopicPartition(TEST_SYSTEM_STREAM_PARTITION, Long.valueOf(TEST_OFFSET));
  }

  @Test
  public void testStartpointOldestVisitorShouldUpdateTheFetchOffsetInConsumer() {
    // Define dummy variables for testing.
    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);
    final KafkaConsumerProxy kafkaConsumerProxy = Mockito.mock(KafkaConsumerProxy.class);
    final KafkaStartpointRegistrationHandler kafkaStartpointRegistrationHandler =  new KafkaStartpointRegistrationHandler(consumer, kafkaConsumerProxy);

    final StartpointOldest testStartpointSpecific = new StartpointOldest();

    // Mock the consumer interactions.
    Mockito.doNothing().when(consumer).seekToBeginning(ImmutableList.of(TEST_TOPIC_PARTITION));
    Mockito.when(consumer.position(TEST_TOPIC_PARTITION)).thenReturn(Long.valueOf(TEST_OFFSET));

    // Invoke the consumer with startpoint.
    kafkaStartpointRegistrationHandler.visit(TEST_SYSTEM_STREAM_PARTITION, testStartpointSpecific);

    // Mock verifications.
    Mockito.verify(consumer).seekToBeginning(ImmutableList.of(TEST_TOPIC_PARTITION));
    Mockito.verify(kafkaConsumerProxy).addTopicPartition(TEST_SYSTEM_STREAM_PARTITION, Long.valueOf(TEST_OFFSET));
  }

  @Test
  public void testStartpointUpcomingVisitorShouldUpdateTheFetchOffsetInConsumer() {
    // Define dummy variables for testing.
    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);
    final KafkaConsumerProxy kafkaConsumerProxy = Mockito.mock(KafkaConsumerProxy.class);
    final KafkaStartpointRegistrationHandler kafkaStartpointRegistrationHandler =  new KafkaStartpointRegistrationHandler(consumer, kafkaConsumerProxy);

    final StartpointUpcoming testStartpointSpecific = new StartpointUpcoming();

    // Mock the consumer interactions.
    Mockito.doNothing().when(consumer).seekToEnd(ImmutableList.of(TEST_TOPIC_PARTITION));
    Mockito.when(consumer.position(TEST_TOPIC_PARTITION)).thenReturn(Long.valueOf(TEST_OFFSET));

    // Invoke the consumer with startpoint.
    kafkaStartpointRegistrationHandler.visit(TEST_SYSTEM_STREAM_PARTITION, testStartpointSpecific);

    // Mock verifications.
    Mockito.verify(consumer).seekToEnd(ImmutableList.of(TEST_TOPIC_PARTITION));
    Mockito.verify(kafkaConsumerProxy).addTopicPartition(TEST_SYSTEM_STREAM_PARTITION, Long.valueOf(TEST_OFFSET));
  }

  @Test
  public void testStartInvocationAfterStartPointsRegistrationShouldInvokeTheStartPointApplyMethod() {
    // Initialize the constants required for the test.
    final Consumer mockConsumer = Mockito.mock(Consumer.class);
    final KafkaSystemConsumerMetrics kafkaSystemConsumerMetrics = new KafkaSystemConsumerMetrics(TEST_SYSTEM, new NoOpMetricsRegistry());

    // Test system stream partitions.
    SystemStreamPartition testSystemStreamPartition1 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(0));
    SystemStreamPartition testSystemStreamPartition2 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(1));
    SystemStreamPartition testSystemStreamPartition3 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(2));
    SystemStreamPartition testSystemStreamPartition4 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(3));

    // Different kinds of {@code Startpoint}.
    StartpointSpecific startPointSpecific = new StartpointSpecific("100");
    StartpointTimestamp startpointTimestamp = new StartpointTimestamp(100L);
    StartpointOldest startpointOldest = new StartpointOldest();
    StartpointUpcoming startpointUpcoming = new StartpointUpcoming();

    // Mock the visit methods of KafkaStartpointRegistrationHandler.
    KafkaSystemConsumer.KafkaStartpointRegistrationHandler mockStartPointVisitor = Mockito.mock(KafkaStartpointRegistrationHandler.class);
    Mockito.doNothing().when(mockStartPointVisitor).visit(testSystemStreamPartition1, startPointSpecific);
    Mockito.doNothing().when(mockStartPointVisitor).visit(testSystemStreamPartition2, startpointTimestamp);
    Mockito.doNothing().when(mockStartPointVisitor).visit(testSystemStreamPartition3, startpointOldest);
    Mockito.doNothing().when(mockStartPointVisitor).visit(testSystemStreamPartition4, startpointUpcoming);

    // Instantiate KafkaSystemConsumer for testing.
    KafkaConsumerProxy proxy = Mockito.mock(KafkaConsumerProxy.class);
    KafkaSystemConsumer kafkaSystemConsumer = new KafkaSystemConsumer(mockConsumer, TEST_SYSTEM, new MapConfig(),
                                                                      TEST_CLIENT_ID, proxy, kafkaSystemConsumerMetrics, new TestClock(), mockStartPointVisitor);


    // Invoke the KafkaSystemConsumer register API with different type of startpoints.
    kafkaSystemConsumer.register(testSystemStreamPartition1, startPointSpecific);
    kafkaSystemConsumer.register(testSystemStreamPartition2, startpointTimestamp);
    kafkaSystemConsumer.register(testSystemStreamPartition3, startpointOldest);
    kafkaSystemConsumer.register(testSystemStreamPartition4, startpointUpcoming);
    kafkaSystemConsumer.start();

    // Mock verifications.
    Mockito.verify(mockStartPointVisitor).visit(testSystemStreamPartition1, startPointSpecific);
    Mockito.verify(mockStartPointVisitor).visit(testSystemStreamPartition2, startpointTimestamp);
    Mockito.verify(mockStartPointVisitor).visit(testSystemStreamPartition3, startpointOldest);
    Mockito.verify(mockStartPointVisitor).visit(testSystemStreamPartition4, startpointUpcoming);
  }

  // mock kafkaConsumer and SystemConsumer
  static class MockKafkaConsumer extends KafkaConsumer {
    public MockKafkaConsumer(Map<String, Object> configs) {
      super(configs);
    }
  }

  static class MockKafkaSystemConsumer extends KafkaSystemConsumer {
    public MockKafkaSystemConsumer(Consumer kafkaConsumer, String systemName, Config config, String clientId,
        KafkaSystemConsumerMetrics metrics, Clock clock) {
      super(kafkaConsumer, systemName, config, clientId, (messageSink) -> mock(KafkaConsumerProxy.class), metrics,
          clock);
    }

    @Override
    void startConsumer() {
    }
  }
}
