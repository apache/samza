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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestKafkaSystemConsumer {
  public final String TEST_SYSTEM = "test-system";
  public final String TEST_STREAM = "test-stream";
  public final String TEST_JOB = "test-job";
  public final String TEST_PREFIX_ID = "testClientId";
  public final String BOOTSTRAP_SERVER = "127.0.0.1:8888";
  public final String FETCH_THRESHOLD_MSGS = "50000";
  public final String FETCH_THRESHOLD_BYTES = "100000";

  @Before
  public void setUp() {

  }

  private KafkaSystemConsumer setupConsumer(String fetchMsg, String fetchBytes) {
    final Map<String, String> map = new HashMap<>();

    map.put(JobConfig.JOB_NAME(), TEST_JOB);

    map.put(String.format(KafkaConfig.CONSUMER_FETCH_THRESHOLD(), TEST_SYSTEM), fetchMsg);
    map.put(String.format(KafkaConfig.CONSUMER_FETCH_THRESHOLD_BYTES(), TEST_SYSTEM), fetchBytes);
    map.put(String.format("systems.%s.consumer.%s", TEST_SYSTEM, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
        BOOTSTRAP_SERVER);

    Config config = new MapConfig(map);
    KafkaConsumerConfig consumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, TEST_SYSTEM, TEST_PREFIX_ID, Collections.emptyMap());
    final KafkaConsumer<byte[], byte[]> kafkaConsumer = new MockKafkaConsumer(consumerConfig.originals());

    MockKafkaSystmeCosumer newKafkaSystemConsumer =
        new MockKafkaSystmeCosumer(kafkaConsumer, TEST_SYSTEM, config, TEST_PREFIX_ID,
            new KafkaSystemConsumerMetrics(TEST_SYSTEM, new NoOpMetricsRegistry()), System::currentTimeMillis);

    return newKafkaSystemConsumer;
  }

  @Test
  public void testConfigValidations() {

    final KafkaSystemConsumer consumer = setupConsumer(FETCH_THRESHOLD_MSGS, FETCH_THRESHOLD_BYTES);

    consumer.start();
    // should be no failures
  }

  @Test
  public void testFetchThresholdShouldDivideEvenlyAmongPartitions() {
    final KafkaSystemConsumer consumer = setupConsumer(FETCH_THRESHOLD_MSGS, FETCH_THRESHOLD_BYTES);
    final int partitionsNum = 50;
    for (int i = 0; i < partitionsNum; i++) {
      consumer.register(new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(i)), "0");
    }

    consumer.start();

    Assert.assertEquals(Long.valueOf(FETCH_THRESHOLD_MSGS) / partitionsNum, consumer.perPartitionFetchThreshold);
    Assert.assertEquals(Long.valueOf(FETCH_THRESHOLD_BYTES) / 2 / partitionsNum,
        consumer.perPartitionFetchThresholdBytes);
  }

  @Test
  public void testConsumerRegisterOlderOffsetOfTheSamzaSSP() {

    KafkaSystemConsumer consumer = setupConsumer(FETCH_THRESHOLD_MSGS, FETCH_THRESHOLD_BYTES);

    SystemStreamPartition ssp0 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(0));
    SystemStreamPartition ssp1 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(1));
    SystemStreamPartition ssp2 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(2));

    consumer.register(ssp0, "0");
    consumer.register(ssp0, "5");
    consumer.register(ssp1, "2");
    consumer.register(ssp1, "3");
    consumer.register(ssp2, "0");

    assertEquals("0", consumer.topicPartitions2Offset.get(KafkaSystemConsumer.toTopicPartition(ssp0)));
    assertEquals("2", consumer.topicPartitions2Offset.get(KafkaSystemConsumer.toTopicPartition(ssp1)));
    assertEquals("0", consumer.topicPartitions2Offset.get(KafkaSystemConsumer.toTopicPartition(ssp2)));
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
    KafkaSystemConsumer consumer = setupConsumer(FETCH_THRESHOLD_MSGS, FETCH_THRESHOLD_BYTES);

    consumer.register(ssp0, "0");
    consumer.register(ssp1, "0");
    consumer.start();
    consumer.messageSink.addMessage(ssp0, ime0);
    // queue for ssp0 should be full now, because we added message of size FETCH_THRESHOLD_MSGS/partitionsNum
    Assert.assertEquals(false, consumer.messageSink.needsMoreMessages(ssp0));
    consumer.messageSink.addMessage(ssp1, ime1);
    // queue for ssp1 should be less then full now, because we added message of size (FETCH_THRESHOLD_MSGS/partitionsNum - 1)
    Assert.assertEquals(true, consumer.messageSink.needsMoreMessages(ssp1));
    consumer.messageSink.addMessage(ssp1, ime11);
    // queue for ssp1 should full now, because we added message of size 20 on top
    Assert.assertEquals(false, consumer.messageSink.needsMoreMessages(ssp1));

    Assert.assertEquals(1, consumer.getNumMessagesInQueue(ssp0));
    Assert.assertEquals(2, consumer.getNumMessagesInQueue(ssp1));
    Assert.assertEquals(ime0Size, consumer.getMessagesSizeInQueue(ssp0));
    Assert.assertEquals(ime1Size + ime11Size, consumer.getMessagesSizeInQueue(ssp1));
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
    KafkaSystemConsumer consumer = setupConsumer("4", "0"); // should disable

    consumer.register(ssp0, "0");
    consumer.register(ssp1, "0");
    consumer.start();
    consumer.messageSink.addMessage(ssp0, ime0);
    // should be full by size, but not full by number of messages (1 of 2)
    Assert.assertEquals(true, consumer.messageSink.needsMoreMessages(ssp0));
    consumer.messageSink.addMessage(ssp1, ime1);
    // not full neither by size nor by messages
    Assert.assertEquals(true, consumer.messageSink.needsMoreMessages(ssp1));
    consumer.messageSink.addMessage(ssp1, ime11);
    // not full by size, but should be full by messages
    Assert.assertEquals(false, consumer.messageSink.needsMoreMessages(ssp1));

    Assert.assertEquals(1, consumer.getNumMessagesInQueue(ssp0));
    Assert.assertEquals(2, consumer.getNumMessagesInQueue(ssp1));
    Assert.assertEquals(ime0Size, consumer.getMessagesSizeInQueue(ssp0));
    Assert.assertEquals(ime1Size + ime11Size, consumer.getMessagesSizeInQueue(ssp1));
  }

  // mock kafkaConsumer and SystemConsumer
  static class MockKafkaConsumer extends KafkaConsumer {
    public MockKafkaConsumer(Map<String, Object> configs) {
      super(configs);
    }
  }

  static class MockKafkaSystmeCosumer extends KafkaSystemConsumer {
    public MockKafkaSystmeCosumer(Consumer kafkaConsumer, String systemName, Config config, String clientId,
        KafkaSystemConsumerMetrics metrics, Clock clock) {
      super(kafkaConsumer, systemName, config, clientId, metrics, clock);
    }

    //@Override
    //void createConsumerProxy() {
    //  this.messageSink = new KafkaConsumerMessageSink();
    //}

    @Override
    void startConsumer() {
    }
  }
}
