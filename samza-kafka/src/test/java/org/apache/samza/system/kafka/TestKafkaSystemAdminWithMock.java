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

package org.apache.samza.system.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import kafka.admin.AdminClient;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.KafkaConsumerConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.samza.system.kafka.KafkaSystemDescriptor.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestKafkaSystemAdminWithMock {
  private KafkaSystemAdmin kafkaSystemAdmin;
  //private KafkaSystemAdmin kafkaAdmin;
  private Config testConfig;
  private Consumer<byte[], byte[]> mockKafkaConsumer;
  private PartitionInfo mockPartitionInfo0;
  private PartitionInfo mockPartitionInfo1;
  private TopicPartition testTopicPartition0;
  private TopicPartition testTopicPartition1;

  private ConcurrentHashMap<String, KafkaSystemConsumer> consumersReference;

  private static final String VALID_TOPIC = "validTopic";
  private static final String INVALID_TOPIC = "invalidTopic";
  private static final String TEST_SYSTEM = "testSystem";
  private static final Long KAFKA_BEGINNING_OFFSET_FOR_PARTITION0 = 10L;
  private static final Long KAFKA_BEGINNING_OFFSET_FOR_PARTITION1 = 11L;
  private static final Long KAFKA_END_OFFSET_FOR_PARTITION0 = 20L;
  private static final Long KAFKA_END_OFFSET_FOR_PARTITION1 = 21L;

  @Before
  public void setUp() throws Exception {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(KafkaConsumerConfig.CONSUMER_CONFIGS_CONFIG_KEY, TEST_SYSTEM, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
        "localhost:123");
    configMap.put(String.format(KafkaConsumerConfig.CONSUMER_ZK_CONNECT_CONFIG_KEY, TEST_SYSTEM), "localhost:124");
    configMap.put(JobConfig.JOB_NAME(), "jobName");
    configMap.put(JobConfig.JOB_ID(), "jobId");

    testConfig = new MapConfig(configMap);

    consumersReference = new ConcurrentHashMap<>();

    // mock PartitionInfo
    mockPartitionInfo0 = mock(PartitionInfo.class);
    when(mockPartitionInfo0.topic()).thenReturn(VALID_TOPIC);
    when(mockPartitionInfo0.partition()).thenReturn(0);
    mockPartitionInfo1 = mock(PartitionInfo.class);
    when(mockPartitionInfo1.topic()).thenReturn(VALID_TOPIC);
    when(mockPartitionInfo1.partition()).thenReturn(1);

    // mock LinkedInKafkaConsumerImpl constructor
    mockKafkaConsumer = mock(KafkaConsumer.class);

    // mock LinkedInKafkaConsumerImpl other behaviors
    testTopicPartition0 = new TopicPartition(VALID_TOPIC, 0);
    testTopicPartition1 = new TopicPartition(VALID_TOPIC, 1);
    Map<TopicPartition, Long> testBeginningOffsets =
        ImmutableMap.of(testTopicPartition0, KAFKA_BEGINNING_OFFSET_FOR_PARTITION0, testTopicPartition1,
            KAFKA_BEGINNING_OFFSET_FOR_PARTITION1);
    Map<TopicPartition, Long> testEndOffsets =
        ImmutableMap.of(testTopicPartition0, KAFKA_END_OFFSET_FOR_PARTITION0, testTopicPartition1,
            KAFKA_END_OFFSET_FOR_PARTITION1);

    when(mockKafkaConsumer.partitionsFor(VALID_TOPIC)).thenReturn(
        ImmutableList.of(mockPartitionInfo0, mockPartitionInfo1));
    when(mockKafkaConsumer.beginningOffsets(ImmutableList.of(testTopicPartition0, testTopicPartition1))).thenReturn(
        testBeginningOffsets);
    when(mockKafkaConsumer.endOffsets(ImmutableList.of(testTopicPartition0, testTopicPartition1))).thenReturn(
        testEndOffsets);

    ZkUtils zkUtilsMock = mock(ZkUtils.class);
    Supplier<ZkUtils> connectZk = () -> zkUtilsMock;

    AdminClient adminClientMock = mock(AdminClient.class);
    Supplier<AdminClient> connectAdminClient = () -> adminClientMock;

    KafkaSystemFactory kafkaSystemFactory = new KafkaSystemFactory();
    kafkaSystemAdmin =
        new KafkaSystemAdmin(TEST_SYSTEM, testConfig, mockKafkaConsumer);

    /*

    KafkaSystemAdmin(String systemName, Supplier<Consumer<K, V>> metadataConsumerSupplier,
        Supplier<ZkUtils> connectZk, Supplier<AdminClient> connectAdminClient,
        Map<String, ChangelogInfo> changelogTopicMetaInformation, Map<String, Properties> intermediateStreamProperties,
        Properties coordinatorStreamProperties, int coordinatorStreamReplicationFactor, boolean deleteCommittedMessages) {
*/
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testGetSystemStreamMetaDataWithValidTopic() {
    System.out.println("STARTING");
    Map<String, SystemStreamMetadata> metadataMap =
        kafkaSystemAdmin.getSystemStreamMetadata(ImmutableSet.of(VALID_TOPIC));

    // verify metadata size
    assertEquals("metadata should return for 1 topic", metadataMap.size(), 1);
    System.out.println("STARTING1");
    // verify the metadata streamName
    assertEquals("the stream name should be " + VALID_TOPIC, metadataMap.get(VALID_TOPIC).getStreamName(), VALID_TOPIC);
    System.out.println("STARTING2");
    // verify the offset for each partition
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> systemStreamPartitionMetadata =
        metadataMap.get(VALID_TOPIC).getSystemStreamPartitionMetadata();
    assertEquals("there are 2 partitions", systemStreamPartitionMetadata.size(), 2);
    System.out.println("STARTING3");
    SystemStreamMetadata.SystemStreamPartitionMetadata partition0Metadata =
        systemStreamPartitionMetadata.get(new Partition(0));
    assertEquals("oldest offset for partition 0", partition0Metadata.getOldestOffset(),
        KAFKA_BEGINNING_OFFSET_FOR_PARTITION0.toString());
    assertEquals("upcoming offset for partition 0", partition0Metadata.getUpcomingOffset(),
        KAFKA_END_OFFSET_FOR_PARTITION0.toString());
    assertEquals("newest offset for partition 0", partition0Metadata.getNewestOffset(),
        Long.toString(KAFKA_END_OFFSET_FOR_PARTITION0 - 1));
    System.out.println("STARTING4");
    SystemStreamMetadata.SystemStreamPartitionMetadata partition1Metadata =
        systemStreamPartitionMetadata.get(new Partition(1));
    assertEquals("oldest offset for partition 1", partition1Metadata.getOldestOffset(),
        KAFKA_BEGINNING_OFFSET_FOR_PARTITION1.toString());
    assertEquals("upcoming offset for partition 1", partition1Metadata.getUpcomingOffset(),
        KAFKA_END_OFFSET_FOR_PARTITION1.toString());
    assertEquals("newest offset for partition 1", partition1Metadata.getNewestOffset(),
        Long.toString(KAFKA_END_OFFSET_FOR_PARTITION1 - 1));
  }

  @Test
  public void testGetSystemStreamMetaDataWithInvalidTopic() {
    Map<String, SystemStreamMetadata> metadataMap =
        kafkaSystemAdmin.getSystemStreamMetadata(ImmutableSet.of(INVALID_TOPIC));
    assertEquals("empty metadata for invalid topic", metadataMap.size(), 0);
  }

  @Test
  public void testGetSystemStreamMetaDataWithNoTopic() {
    Map<String, SystemStreamMetadata> metadataMap = kafkaSystemAdmin.getSystemStreamMetadata(Collections.emptySet());
    assertEquals("empty metadata for no topic", metadataMap.size(), 0);
  }

  @Test
  public void testGetSystemStreamMetaDataForTopicWithNoMessage() {
    // The topic with no messages will have beginningOffset = 0 and endOffset = 0
    when(mockKafkaConsumer.beginningOffsets(ImmutableList.of(testTopicPartition0, testTopicPartition1))).thenReturn(
        ImmutableMap.of(testTopicPartition0, 0L, testTopicPartition1, 0L));
    when(mockKafkaConsumer.endOffsets(ImmutableList.of(testTopicPartition0, testTopicPartition1))).thenReturn(
        ImmutableMap.of(testTopicPartition0, 0L, testTopicPartition1, 0L));

    Map<String, SystemStreamMetadata> metadataMap =
        kafkaSystemAdmin.getSystemStreamMetadata(ImmutableSet.of(VALID_TOPIC));
    assertEquals("metadata should return for 1 topic", metadataMap.size(), 1);

    // verify the metadata streamName
    assertEquals("the stream name should be " + VALID_TOPIC, metadataMap.get(VALID_TOPIC).getStreamName(), VALID_TOPIC);

    // verify the offset for each partition
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> systemStreamPartitionMetadata =
        metadataMap.get(VALID_TOPIC).getSystemStreamPartitionMetadata();
    assertEquals("there are 2 partitions", systemStreamPartitionMetadata.size(), 2);

    SystemStreamMetadata.SystemStreamPartitionMetadata partition0Metadata =
        systemStreamPartitionMetadata.get(new Partition(0));
    assertEquals("oldest offset for partition 0", partition0Metadata.getOldestOffset(), "0");
    assertEquals("upcoming offset for partition 0", partition0Metadata.getUpcomingOffset(), "0");
    assertEquals("newest offset is not set due to abnormal upcoming offset", partition0Metadata.getNewestOffset(),
        null);

    SystemStreamMetadata.SystemStreamPartitionMetadata partition1Metadata =
        systemStreamPartitionMetadata.get(new Partition(1));
    assertEquals("oldest offset for partition 1", partition1Metadata.getOldestOffset(), "0");
    assertEquals("upcoming offset for partition 1", partition1Metadata.getUpcomingOffset(), "0");
    assertEquals("newest offset is not set due to abnormal upcoming offset", partition1Metadata.getNewestOffset(),
        null);
  }

  @Test
  public void testGetSSPMetadata() {
    SystemStreamPartition ssp = new SystemStreamPartition(TEST_SYSTEM, VALID_TOPIC, new Partition(0));
    SystemStreamPartition otherSSP = new SystemStreamPartition(TEST_SYSTEM, "otherTopic", new Partition(1));
    TopicPartition topicPartition = new TopicPartition(VALID_TOPIC, 0);
    TopicPartition otherTopicPartition = new TopicPartition("otherTopic", 1);
    when(mockKafkaConsumer.beginningOffsets(ImmutableList.of(topicPartition, otherTopicPartition))).thenReturn(
        ImmutableMap.of(topicPartition, 1L, otherTopicPartition, 2L));
    when(mockKafkaConsumer.endOffsets(ImmutableList.of(topicPartition, otherTopicPartition))).thenReturn(
        ImmutableMap.of(topicPartition, 11L, otherTopicPartition, 12L));
    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> expected =
        ImmutableMap.of(ssp, new SystemStreamMetadata.SystemStreamPartitionMetadata("1", "10", "11"), otherSSP,
            new SystemStreamMetadata.SystemStreamPartitionMetadata("2", "11", "12"));
    assertEquals(kafkaSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp, otherSSP)), expected);
  }

  @Test
  public void testGetSSPMetadataEmptyPartition() {
    SystemStreamPartition ssp = new SystemStreamPartition(TEST_SYSTEM, VALID_TOPIC, new Partition(0));
    SystemStreamPartition otherSSP = new SystemStreamPartition(TEST_SYSTEM, "otherTopic", new Partition(1));
    TopicPartition topicPartition = new TopicPartition(VALID_TOPIC, 0);
    TopicPartition otherTopicPartition = new TopicPartition("otherTopic", 1);
    when(mockKafkaConsumer.beginningOffsets(ImmutableList.of(topicPartition, otherTopicPartition))).thenReturn(
        ImmutableMap.of(topicPartition, 1L));
    when(mockKafkaConsumer.endOffsets(ImmutableList.of(topicPartition, otherTopicPartition))).thenReturn(
        ImmutableMap.of(topicPartition, 11L));

    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> expected =
        ImmutableMap.of(ssp, new SystemStreamMetadata.SystemStreamPartitionMetadata("1", "10", "11"), otherSSP,
            new SystemStreamMetadata.SystemStreamPartitionMetadata(null, null, null));
    assertEquals(expected, kafkaSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp, otherSSP)));
  }

  @Test
  public void testGetSSPMetadataEmptyUpcomingOffset() {
    SystemStreamPartition ssp = new SystemStreamPartition(TEST_SYSTEM, VALID_TOPIC, new Partition(0));
    TopicPartition topicPartition = new TopicPartition(VALID_TOPIC, 0);
    when(mockKafkaConsumer.beginningOffsets(ImmutableList.of(topicPartition))).thenReturn(
        ImmutableMap.of(topicPartition, 0L));
    when(mockKafkaConsumer.endOffsets(ImmutableList.of(topicPartition))).thenReturn(ImmutableMap.of());
    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> expected =
        ImmutableMap.of(ssp, new SystemStreamMetadata.SystemStreamPartitionMetadata("0", null, null));
    assertEquals(kafkaSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp)), expected);
  }

  @Test
  public void testGetSSPMetadataZeroUpcomingOffset() {
    SystemStreamPartition ssp = new SystemStreamPartition(TEST_SYSTEM, VALID_TOPIC, new Partition(0));
    TopicPartition topicPartition = new TopicPartition(VALID_TOPIC, 0);
    when(mockKafkaConsumer.beginningOffsets(ImmutableList.of(topicPartition))).thenReturn(
        ImmutableMap.of(topicPartition, -1L));
    when(mockKafkaConsumer.endOffsets(ImmutableList.of(topicPartition))).thenReturn(
        ImmutableMap.of(topicPartition, 0L));
    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> expected =
        ImmutableMap.of(ssp, new SystemStreamMetadata.SystemStreamPartitionMetadata("0", null, "0"));
    assertEquals(kafkaSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp)), expected);
  }

  @Test
  public void testGetSystemStreamMetaDataWithRetry() {
    final List<PartitionInfo> partitionInfosForTopic = ImmutableList.of(mockPartitionInfo0, mockPartitionInfo1);
    when(mockKafkaConsumer.partitionsFor(VALID_TOPIC)).thenThrow(new RuntimeException())
        .thenReturn(partitionInfosForTopic);

    Map<String, SystemStreamMetadata> metadataMap =
        kafkaSystemAdmin.getSystemStreamMetadata(ImmutableSet.of(VALID_TOPIC));
    assertEquals("metadata should return for 1 topic", metadataMap.size(), 1);

    // retried twice because the first fails and the second succeeds
    Mockito.verify(mockKafkaConsumer, Mockito.times(2)).partitionsFor(VALID_TOPIC);

    final List<TopicPartition> topicPartitions =
        Arrays.asList(new TopicPartition(mockPartitionInfo0.topic(), mockPartitionInfo0.partition()),
            new TopicPartition(mockPartitionInfo1.topic(), mockPartitionInfo1.partition()));
    // the following methods thereafter are only called once
    Mockito.verify(mockKafkaConsumer, Mockito.times(1)).beginningOffsets(topicPartitions);
    Mockito.verify(mockKafkaConsumer, Mockito.times(1)).endOffsets(topicPartitions);
  }

  @Test(expected = SamzaException.class)
  public void testGetSystemStreamMetadataShouldTerminateAfterFiniteRetriesOnException() {
    when(mockKafkaConsumer.partitionsFor(VALID_TOPIC)).thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException());

    kafkaSystemAdmin.getSystemStreamMetadata(ImmutableSet.of(VALID_TOPIC));
  }

  @Test(expected = SamzaException.class)
  public void testGetSystemStreamPartitionCountsShouldTerminateAfterFiniteRetriesOnException() throws Exception {
    final Set<String> streamNames = ImmutableSet.of(VALID_TOPIC);
    final long cacheTTL = 100L;

    when(mockKafkaConsumer.partitionsFor(VALID_TOPIC)).thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException());

    kafkaSystemAdmin.getSystemStreamPartitionCounts(streamNames, cacheTTL);
  }
}
