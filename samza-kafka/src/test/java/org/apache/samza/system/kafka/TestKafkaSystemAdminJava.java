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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.samza.Partition;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.startpoint.StartpointOldest;
import org.apache.samza.startpoint.StartpointSpecific;
import org.apache.samza.startpoint.StartpointTimestamp;
import org.apache.samza.startpoint.StartpointUpcoming;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.kafka.KafkaSystemAdmin.KafkaStartpointToOffsetResolver;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.samza.system.kafka.KafkaSystemAdmin.*;
import static org.junit.Assert.*;

public class TestKafkaSystemAdminJava extends TestKafkaSystemAdmin {
  private static final String SYSTEM = "kafka";
  private static final String TOPIC = "input";
  private static final String TEST_SYSTEM = "test-system";
  private static final String TEST_STREAM = "test-stream";
  private static final Integer TEST_PARTITION_ID = 0;
  private static final TopicPartition TEST_TOPIC_PARTITION = new TopicPartition(TEST_STREAM, TEST_PARTITION_ID);
  private static final Partition TEST_PARTITION = new Partition(TEST_PARTITION_ID);
  private static final SystemStreamPartition TEST_SYSTEM_STREAM_PARTITION = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, TEST_PARTITION);
  private static final String TEST_OFFSET = "10";

  @Test
  public void testCreateStreamShouldCoordinatorStreamWithCorrectTopicProperties() throws Exception {
    String coordinatorTopicName = String.format("topic-name-%s", RandomStringUtils.randomAlphabetic(5));
    StreamSpec coordinatorStreamSpec = KafkaStreamSpec.createCoordinatorStreamSpec(coordinatorTopicName, SYSTEM());

    boolean hasCreatedStream = systemAdmin().createStream(coordinatorStreamSpec);

    assertTrue(hasCreatedStream);

    Map<String, String> coordinatorTopicProperties = getTopicConfigFromKafkaBroker(coordinatorTopicName);

    assertEquals("compact", coordinatorTopicProperties.get(TopicConfig.CLEANUP_POLICY_CONFIG));
    assertEquals("26214400", coordinatorTopicProperties.get(TopicConfig.SEGMENT_BYTES_CONFIG));
    assertEquals("86400000", coordinatorTopicProperties.get(TopicConfig.DELETE_RETENTION_MS_CONFIG));
  }

  private static Map<String, String> getTopicConfigFromKafkaBroker(String topicName) throws Exception {
    List<ConfigResource> configResourceList = ImmutableList.of(
        new ConfigResource(ConfigResource.Type.TOPIC, topicName));
    Map<ConfigResource, org.apache.kafka.clients.admin.Config> configResourceConfigMap =
        adminClient().describeConfigs(configResourceList).all().get();
    Map<String, String> kafkaTopicConfig = new HashMap<>();

    configResourceConfigMap.values().forEach(configEntry -> {
      configEntry.entries().forEach(config -> {
        kafkaTopicConfig.put(config.name(), config.value());
      });
    });

    return kafkaTopicConfig;
  }

  @Test
  public void testGetOffsetsAfter() {
    SystemStreamPartition ssp1 = new SystemStreamPartition(SYSTEM, TOPIC, new Partition(0));
    SystemStreamPartition ssp2 = new SystemStreamPartition(SYSTEM, TOPIC, new Partition(1));
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    offsets.put(ssp1, "1");
    offsets.put(ssp2, "2");

    offsets = systemAdmin().getOffsetsAfter(offsets);

    Assert.assertEquals("2", offsets.get(ssp1));
    Assert.assertEquals("3", offsets.get(ssp2));
  }

  @Test
  public void testToKafkaSpecForCheckpointStreamShouldReturnTheCorrectStreamSpecByPreservingTheConfig() {
    String topicName = "testStream";
    String streamId = "samza-internal-checkpoint-stream-id";
    int partitionCount = 1;
    Map<String, String> map = new HashMap<>();
    map.put("cleanup.policy", "compact");
    map.put("replication.factor", "3");
    map.put("segment.bytes", "536870912");
    map.put("delete.retention.ms", "86400000");

    Config config = new MapConfig(map);

    StreamSpec spec = new StreamSpec(streamId, topicName, SYSTEM, partitionCount, config);
    KafkaSystemAdmin kafkaSystemAdmin = systemAdmin();
    KafkaStreamSpec kafkaStreamSpec = kafkaSystemAdmin.toKafkaSpec(spec);
    System.out.println(kafkaStreamSpec);
    assertEquals(streamId, kafkaStreamSpec.getId());
    assertEquals(topicName, kafkaStreamSpec.getPhysicalName());
    assertEquals(partitionCount, kafkaStreamSpec.getPartitionCount());
    assertEquals(3, kafkaStreamSpec.getReplicationFactor());
    assertEquals("compact", kafkaStreamSpec.getConfig().get("cleanup.policy"));
    assertEquals("536870912", kafkaStreamSpec.getConfig().get("segment.bytes"));
    assertEquals("86400000", kafkaStreamSpec.getConfig().get("delete.retention.ms"));
  }

  @Test
  public void testToKafkaSpec() {
    String topicName = "testStream";

    int defaultPartitionCount = 2;
    int changeLogPartitionFactor = 5;
    Map<String, String> map = new HashMap<>();
    Config config = new MapConfig(map);
    StreamSpec spec = new StreamSpec("id", topicName, SYSTEM, defaultPartitionCount, config);

    KafkaSystemAdmin kafkaAdmin = systemAdmin();
    KafkaStreamSpec kafkaSpec = kafkaAdmin.toKafkaSpec(spec);

    Assert.assertEquals("id", kafkaSpec.getId());
    Assert.assertEquals(topicName, kafkaSpec.getPhysicalName());
    Assert.assertEquals(SYSTEM, kafkaSpec.getSystemName());
    Assert.assertEquals(defaultPartitionCount, kafkaSpec.getPartitionCount());

    // validate that conversion is using coordination metadata
    map.put("job.coordinator.segment.bytes", "123");
    map.put("job.coordinator.cleanup.policy", "superCompact");
    int coordReplicatonFactor = 4;
    map.put(org.apache.samza.config.KafkaConfig.JOB_COORDINATOR_REPLICATION_FACTOR(),
        String.valueOf(coordReplicatonFactor));

    KafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(SYSTEM, map));
    spec = StreamSpec.createCoordinatorStreamSpec(topicName, SYSTEM);
    kafkaSpec = admin.toKafkaSpec(spec);
    Assert.assertEquals(coordReplicatonFactor, kafkaSpec.getReplicationFactor());
    Assert.assertEquals("123", kafkaSpec.getProperties().getProperty("segment.bytes"));
    // cleanup policy is overridden in the KafkaAdmin
    Assert.assertEquals("compact", kafkaSpec.getProperties().getProperty("cleanup.policy"));

    // validate that conversion is using changeLog metadata
    map = new HashMap<>();
    map.put(JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM);

    map.put(String.format("stores.%s.changelog", "fakeStore"), topicName);
    int changeLogReplicationFactor = 3;
    map.put(String.format("stores.%s.changelog.replication.factor", "fakeStore"),
        String.valueOf(changeLogReplicationFactor));
    admin = Mockito.spy(createSystemAdmin(SYSTEM, map));
    spec = StreamSpec.createChangeLogStreamSpec(topicName, SYSTEM, changeLogPartitionFactor);
    kafkaSpec = admin.toKafkaSpec(spec);
    Assert.assertEquals(changeLogReplicationFactor, kafkaSpec.getReplicationFactor());

    // same, but with missing topic info
    try {
      admin = Mockito.spy(createSystemAdmin(SYSTEM, map));
      spec = StreamSpec.createChangeLogStreamSpec("anotherTopic", SYSTEM, changeLogPartitionFactor);
      kafkaSpec = admin.toKafkaSpec(spec);
      Assert.fail("toKafkaSpec should've failed for missing topic");
    } catch (StreamValidationException e) {
      // expected
    }

    // validate that conversion is using intermediate streams properties
    String interStreamId = "isId";

    Map<String, String> interStreamMap = new HashMap<>();
    interStreamMap.put("app.mode", ApplicationConfig.ApplicationMode.BATCH.toString());
    interStreamMap.put(String.format("streams.%s.samza.intermediate", interStreamId), "true");
    interStreamMap.put(String.format("streams.%s.samza.system", interStreamId), "testSystem");
    interStreamMap.put(String.format("streams.%s.p1", interStreamId), "v1");
    interStreamMap.put(String.format("streams.%s.retention.ms", interStreamId), "123");
    // legacy format
    interStreamMap.put(String.format("systems.%s.streams.%s.p2", "testSystem", interStreamId), "v2");

    admin = Mockito.spy(createSystemAdmin(SYSTEM, interStreamMap));
    spec = new StreamSpec(interStreamId, topicName, SYSTEM, defaultPartitionCount, config);
    kafkaSpec = admin.toKafkaSpec(spec);
    Assert.assertEquals("v1", kafkaSpec.getProperties().getProperty("p1"));
    Assert.assertEquals("v2", kafkaSpec.getProperties().getProperty("p2"));
    Assert.assertEquals("123", kafkaSpec.getProperties().getProperty("retention.ms"));
    Assert.assertEquals(defaultPartitionCount, kafkaSpec.getPartitionCount());
  }

  @Test
  public void testCreateCoordinatorStream() {
    SystemAdmin admin = Mockito.spy(systemAdmin());
    StreamSpec spec = StreamSpec.createCoordinatorStreamSpec("testCoordinatorStream", "testSystem");

    admin.createStream(spec);
    admin.validateStream(spec);
    Mockito.verify(admin).createStream(Mockito.any());
  }

  @Test
  public void testCreateCoordinatorStreamWithSpecialCharsInTopicName() {
    final String stream = "test.coordinator_test.Stream";

    Map<String, String> map = new HashMap<>();
    map.put("job.coordinator.segment.bytes", "123");
    map.put("job.coordinator.cleanup.policy", "compact");
    int coordReplicatonFactor = 2;
    map.put(org.apache.samza.config.KafkaConfig.JOB_COORDINATOR_REPLICATION_FACTOR(),
        String.valueOf(coordReplicatonFactor));

    KafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(SYSTEM, map));
    StreamSpec spec = StreamSpec.createCoordinatorStreamSpec(stream, SYSTEM);

    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isCoordinatorStream());
      assertEquals(SYSTEM, internalSpec.getSystemName());
      assertEquals(stream, internalSpec.getPhysicalName());
      assertEquals(1, internalSpec.getPartitionCount());
      Assert.assertEquals(coordReplicatonFactor, ((KafkaStreamSpec) internalSpec).getReplicationFactor());
      Assert.assertEquals("123", ((KafkaStreamSpec) internalSpec).getProperties().getProperty("segment.bytes"));
      // cleanup policy is overridden in the KafkaAdmin
      Assert.assertEquals("compact", ((KafkaStreamSpec) internalSpec).getProperties().getProperty("cleanup.policy"));

      return internalSpec;
    }).when(admin).toKafkaSpec(Mockito.any());

    admin.createStream(spec);
    admin.validateStream(spec);
  }

  @Test
  public void testCreateChangelogStreamHelp() {
    testCreateChangelogStreamHelp("testChangeLogStream");
  }

  @Test
  public void testCreateChangelogStreamWithSpecialCharsInTopicName() {
    // cannot contain period
    testCreateChangelogStreamHelp("test-Change_Log-Stream");
  }

  public void testCreateChangelogStreamHelp(final String topic) {
    final int partitions = 12;
    final int repFactor = 2;

    Map<String, String> map = new HashMap<>();
    map.put(JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM);
    map.put(String.format("stores.%s.changelog", "fakeStore"), topic);
    map.put(String.format("stores.%s.changelog.replication.factor", "fakeStore"), String.valueOf(repFactor));
    map.put(String.format("stores.%s.changelog.kafka.segment.bytes", "fakeStore"), "139");
    KafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(SYSTEM, map));
    StreamSpec spec = StreamSpec.createChangeLogStreamSpec(topic, SYSTEM, partitions);

    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isChangeLogStream());
      assertEquals(SYSTEM, internalSpec.getSystemName());
      assertEquals(topic, internalSpec.getPhysicalName());
      assertEquals(repFactor, ((KafkaStreamSpec) internalSpec).getReplicationFactor());
      assertEquals(partitions, internalSpec.getPartitionCount());
      assertEquals("139", ((KafkaStreamSpec) internalSpec).getProperties().getProperty("segment.bytes"));
      assertEquals("compact", ((KafkaStreamSpec) internalSpec).getProperties().getProperty("cleanup.policy"));

      return internalSpec;
    }).when(admin).toKafkaSpec(Mockito.any());

    admin.createStream(spec);
    admin.validateStream(spec);
  }

  @Test
  public void testCreateStream() {
    StreamSpec spec = new StreamSpec("testId", "testStream", "testSystem", 8);
    KafkaSystemAdmin admin = systemAdmin();
    assertTrue("createStream should return true if the stream does not exist and then is created.",
        admin.createStream(spec));
    admin.validateStream(spec);

    assertFalse("createStream should return false if the stream already exists.", systemAdmin().createStream(spec));
  }

  @Test(expected = StreamValidationException.class)
  public void testValidateStreamDoesNotExist() {

    StreamSpec spec = new StreamSpec("testId", "testStreamNameExist", "testSystem", 8);

    systemAdmin().validateStream(spec);
  }

  @Test(expected = StreamValidationException.class)
  public void testValidateStreamWrongPartitionCount() {
    StreamSpec spec1 = new StreamSpec("testId", "testStreamPartition", "testSystem", 8);
    StreamSpec spec2 = new StreamSpec("testId", "testStreamPartition", "testSystem", 4);

    assertTrue("createStream should return true if the stream does not exist and then is created.",
        systemAdmin().createStream(spec1));

    systemAdmin().validateStream(spec2);
  }

  @Test(expected = StreamValidationException.class)
  public void testValidateStreamWrongName() {
    StreamSpec spec1 = new StreamSpec("testId", "testStreamName1", "testSystem", 8);
    StreamSpec spec2 = new StreamSpec("testId", "testStreamName2", "testSystem", 8);

    assertTrue("createStream should return true if the stream does not exist and then is created.",
        systemAdmin().createStream(spec1));

    systemAdmin().validateStream(spec2);
  }

  @Test
  public void testClearStream() {
    StreamSpec spec = new StreamSpec("testId", "testStreamClear", "testSystem", 8);

    KafkaSystemAdmin admin = systemAdmin();
    String topicName = spec.getPhysicalName();

    assertTrue("createStream should return true if the stream does not exist and then is created.", admin.createStream(spec));
    // validate topic exists
    assertTrue(admin.clearStream(spec));

    // validate that topic was removed
    DescribeTopicsResult dtr = admin.adminClient.describeTopics(ImmutableSet.of(topicName));
    try {
      TopicDescription td = dtr.all().get().get(topicName);
      Assert.fail("topic " + topicName + " should've been removed. td=" + td);
    } catch (Exception e) {
      if (!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)) {
        Assert.fail("topic " + topicName + " should've been removed. Expected UnknownTopicOrPartitionException.");
      }
    }
  }

  @Test
  public void testShouldAssembleMetadata() {
    Map<SystemStreamPartition, String> oldestOffsets = new ImmutableMap.Builder<SystemStreamPartition, String>()
        .put(new SystemStreamPartition(SYSTEM, "stream1", new Partition(0)), "o1")
        .put(new SystemStreamPartition(SYSTEM, "stream2", new Partition(0)), "o2")
        .put(new SystemStreamPartition(SYSTEM, "stream1", new Partition(1)), "o3")
        .put(new SystemStreamPartition(SYSTEM, "stream2", new Partition(1)), "o4")
        .build();

    Map<SystemStreamPartition, String> newestOffsets = new ImmutableMap.Builder<SystemStreamPartition, String>()
        .put(new SystemStreamPartition(SYSTEM, "stream1", new Partition(0)), "n1")
        .put(new SystemStreamPartition(SYSTEM, "stream2", new Partition(0)), "n2")
        .put(new SystemStreamPartition(SYSTEM, "stream1", new Partition(1)), "n3")
        .put(new SystemStreamPartition(SYSTEM, "stream2", new Partition(1)), "n4")
        .build();

    Map<SystemStreamPartition, String> upcomingOffsets = new ImmutableMap.Builder<SystemStreamPartition, String>()
        .put(new SystemStreamPartition(SYSTEM, "stream1", new Partition(0)), "u1")
        .put(new SystemStreamPartition(SYSTEM, "stream2", new Partition(0)), "u2")
        .put(new SystemStreamPartition(SYSTEM, "stream1", new Partition(1)), "u3")
        .put(new SystemStreamPartition(SYSTEM, "stream2", new Partition(1)), "u4")
        .build();

    Map<String, SystemStreamMetadata> metadata = assembleMetadata(oldestOffsets, newestOffsets, upcomingOffsets);
    assertNotNull(metadata);
    assertEquals(2, metadata.size());
    assertTrue(metadata.containsKey("stream1"));
    assertTrue(metadata.containsKey("stream2"));
    SystemStreamMetadata stream1Metadata = metadata.get("stream1");
    SystemStreamMetadata stream2Metadata = metadata.get("stream2");
    assertNotNull(stream1Metadata);
    assertNotNull(stream2Metadata);
    assertEquals("stream1", stream1Metadata.getStreamName());
    assertEquals("stream2", stream2Metadata.getStreamName());
    SystemStreamMetadata.SystemStreamPartitionMetadata expectedSystemStream1Partition0Metadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("o1", "n1", "u1");
    SystemStreamMetadata.SystemStreamPartitionMetadata expectedSystemStream1Partition1Metadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("o3", "n3", "u3");
    SystemStreamMetadata.SystemStreamPartitionMetadata expectedSystemStream2Partition0Metadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("o2", "n2", "u2");
    SystemStreamMetadata.SystemStreamPartitionMetadata expectedSystemStream2Partition1Metadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("o4", "n4", "u4");
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> stream1PartitionMetadata =
        stream1Metadata.getSystemStreamPartitionMetadata();
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> stream2PartitionMetadata =
        stream2Metadata.getSystemStreamPartitionMetadata();
    assertEquals(expectedSystemStream1Partition0Metadata, stream1PartitionMetadata.get(new Partition(0)));
    assertEquals(expectedSystemStream1Partition1Metadata, stream1PartitionMetadata.get(new Partition(1)));
    assertEquals(expectedSystemStream2Partition0Metadata, stream2PartitionMetadata.get(new Partition(0)));
    assertEquals(expectedSystemStream2Partition1Metadata, stream2PartitionMetadata.get(new Partition(1)));
  }

  @Test
  public void testStartpointSpecificOffsetVisitorShouldResolveToCorrectOffset() {
    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);
    final KafkaStartpointToOffsetResolver kafkaStartpointToOffsetResolver = new KafkaStartpointToOffsetResolver(consumer);

    final StartpointSpecific testStartpointSpecific = new StartpointSpecific(TEST_OFFSET);

    // Invoke the consumer with startpoint.
    String resolvedOffset = kafkaStartpointToOffsetResolver.visit(TEST_SYSTEM_STREAM_PARTITION, testStartpointSpecific);
    Assert.assertEquals(TEST_OFFSET, resolvedOffset);
  }

  @Test
  public void testStartpointTimestampVisitorShouldResolveToCorrectOffset() {
    // Define dummy variables for testing.
    final Long testTimeStamp = 10L;

    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);

    final KafkaStartpointToOffsetResolver kafkaStartpointToOffsetResolver = new KafkaStartpointToOffsetResolver(consumer);

    final StartpointTimestamp startpointTimestamp = new StartpointTimestamp(testTimeStamp);
    final Map<TopicPartition, OffsetAndTimestamp> offsetForTimesResult = ImmutableMap.of(
        TEST_TOPIC_PARTITION, new OffsetAndTimestamp(Long.valueOf(TEST_OFFSET), testTimeStamp));

    // Mock the consumer interactions.
    Mockito.when(consumer.offsetsForTimes(ImmutableMap.of(TEST_TOPIC_PARTITION, testTimeStamp))).thenReturn(offsetForTimesResult);
    Mockito.when(consumer.position(TEST_TOPIC_PARTITION)).thenReturn(Long.valueOf(TEST_OFFSET));

    String resolvedOffset = kafkaStartpointToOffsetResolver.visit(TEST_SYSTEM_STREAM_PARTITION, startpointTimestamp);
    Assert.assertEquals(TEST_OFFSET, resolvedOffset);
  }

  @Test
  public void testStartpointTimestampVisitorShouldResolveToCorrectOffsetWhenTimestampDoesNotExist() {
    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);
    final KafkaStartpointToOffsetResolver kafkaStartpointToOffsetResolver = new KafkaStartpointToOffsetResolver(consumer);

    final StartpointTimestamp startpointTimestamp = new StartpointTimestamp(0L);
    final Map<TopicPartition, OffsetAndTimestamp> offsetForTimesResult = new HashMap<>();
    offsetForTimesResult.put(TEST_TOPIC_PARTITION, null);

    // Mock the consumer interactions.
    Mockito.when(consumer.offsetsForTimes(ImmutableMap.of(TEST_TOPIC_PARTITION, 0L))).thenReturn(offsetForTimesResult);
    Mockito.when(consumer.endOffsets(ImmutableSet.of(TEST_TOPIC_PARTITION))).thenReturn(ImmutableMap.of(TEST_TOPIC_PARTITION, 10L));

    String resolvedOffset = kafkaStartpointToOffsetResolver.visit(TEST_SYSTEM_STREAM_PARTITION, startpointTimestamp);
    Assert.assertEquals(TEST_OFFSET, resolvedOffset);

    // Mock verifications.
    Mockito.verify(consumer).offsetsForTimes(ImmutableMap.of(TEST_TOPIC_PARTITION, 0L));
  }

  @Test
  public void testStartpointOldestVisitorShouldResolveToCorrectOffset() {
    // Define dummy variables for testing.
    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);
    final KafkaStartpointToOffsetResolver kafkaStartpointToOffsetResolver = new KafkaStartpointToOffsetResolver(consumer);

    final StartpointOldest testStartpointSpecific = new StartpointOldest();

    // Mock the consumer interactions.
    Mockito.when(consumer.beginningOffsets(ImmutableSet.of(TEST_TOPIC_PARTITION))).thenReturn(ImmutableMap.of(TEST_TOPIC_PARTITION, 10L));

    // Invoke the consumer with startpoint.
    String resolvedOffset = kafkaStartpointToOffsetResolver.visit(TEST_SYSTEM_STREAM_PARTITION, testStartpointSpecific);
    Assert.assertEquals(TEST_OFFSET, resolvedOffset);
  }

  @Test
  public void testStartpointUpcomingVisitorShouldResolveToCorrectOffset() {
    // Define dummy variables for testing.
    final KafkaConsumer consumer = Mockito.mock(KafkaConsumer.class);

    final KafkaStartpointToOffsetResolver kafkaStartpointToOffsetResolver = new KafkaStartpointToOffsetResolver(consumer);

    final StartpointUpcoming testStartpointSpecific = new StartpointUpcoming();

    // Mock the consumer interactions.
    Mockito.when(consumer.endOffsets(ImmutableSet.of(TEST_TOPIC_PARTITION))).thenReturn(ImmutableMap.of(TEST_TOPIC_PARTITION, 10L));

    // Invoke the consumer with startpoint.
    String resolvedOffset = kafkaStartpointToOffsetResolver.visit(TEST_SYSTEM_STREAM_PARTITION, testStartpointSpecific);
    Assert.assertEquals(TEST_OFFSET, resolvedOffset);
  }
}
