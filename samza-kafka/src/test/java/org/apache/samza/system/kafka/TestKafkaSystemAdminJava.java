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

import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.api.TopicMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.samza.Partition;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;


public class TestKafkaSystemAdminJava extends TestKafkaSystemAdmin {

  @Test
  public void testGetOffsetsAfter() {
    SystemStreamPartition ssp1 = new SystemStreamPartition(SYSTEM(), TOPIC(), new Partition(0));
    SystemStreamPartition ssp2 = new SystemStreamPartition(SYSTEM(), TOPIC(), new Partition(1));
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    offsets.put(ssp1, "1");
    offsets.put(ssp2, "2");

    offsets = systemAdmin().getOffsetsAfter(offsets);

    Assert.assertEquals("2", offsets.get(ssp1));
    Assert.assertEquals("3", offsets.get(ssp2));
  }

  @Test
  public void testToKafkaSpec() {
    String topicName = "testStream";

    int defaultPartitionCount = 2;
    int changeLogPartitionFactor = 5;
    Map<String, String> map = new HashMap<>();
    Config config = new MapConfig(map);
    StreamSpec spec = new StreamSpec("id", topicName, SYSTEM(), defaultPartitionCount, config);

    KafkaSystemAdmin kafkaAdmin = systemAdmin();
    KafkaStreamSpec kafkaSpec = kafkaAdmin.toKafkaSpec(spec);

    Assert.assertEquals("id", kafkaSpec.getId());
    Assert.assertEquals(topicName, kafkaSpec.getPhysicalName());
    Assert.assertEquals(SYSTEM(), kafkaSpec.getSystemName());
    Assert.assertEquals(defaultPartitionCount, kafkaSpec.getPartitionCount());

    // validate that conversion is using coordination metadata
    map.put("job.coordinator.segment.bytes", "123");
    map.put("job.coordinator.cleanup.policy", "superCompact");
    int coordReplicatonFactor = 4;
    map.put(org.apache.samza.config.KafkaConfig.JOB_COORDINATOR_REPLICATION_FACTOR(),
        String.valueOf(coordReplicatonFactor));

    KafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(SYSTEM(), map));
    spec = StreamSpec.createCoordinatorStreamSpec(topicName, SYSTEM());
    kafkaSpec = admin.toKafkaSpec(spec);
    Assert.assertEquals(coordReplicatonFactor, kafkaSpec.getReplicationFactor());
    Assert.assertEquals("123", kafkaSpec.getProperties().getProperty("segment.bytes"));
    // cleanup policy is overridden in the KafkaAdmin
    Assert.assertEquals("compact", kafkaSpec.getProperties().getProperty("cleanup.policy"));

    // validate that conversion is using changeLog metadata
    map = new HashMap<>();
    map.put(JobConfig.JOB_DEFAULT_SYSTEM(), SYSTEM());

    map.put(String.format("stores.%s.changelog", "fakeStore"), topicName);
    int changeLogReplicationFactor = 3;
    map.put(String.format("stores.%s.changelog.replication.factor", "fakeStore"),
        String.valueOf(changeLogReplicationFactor));
    admin = Mockito.spy(createSystemAdmin(SYSTEM(), map));
    spec = StreamSpec.createChangeLogStreamSpec(topicName, SYSTEM(), changeLogPartitionFactor);
    kafkaSpec = admin.toKafkaSpec(spec);
    Assert.assertEquals(changeLogReplicationFactor, kafkaSpec.getReplicationFactor());

    // same, but with missing topic info
    try {
      admin = Mockito.spy(createSystemAdmin(SYSTEM(), map));
      spec = StreamSpec.createChangeLogStreamSpec("anotherTopic", SYSTEM(), changeLogPartitionFactor);
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

    admin = Mockito.spy(createSystemAdmin(SYSTEM(), interStreamMap));
    spec = new StreamSpec(interStreamId, topicName, SYSTEM(), defaultPartitionCount, config);
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
    final String STREAM = "test.coordinator_test.Stream";

    Map<String, String> map = new HashMap<>();
    map.put("job.coordinator.segment.bytes", "123");
    map.put("job.coordinator.cleanup.policy", "compact");
    int coordReplicatonFactor = 2;
    map.put(org.apache.samza.config.KafkaConfig.JOB_COORDINATOR_REPLICATION_FACTOR(),
        String.valueOf(coordReplicatonFactor));

    KafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(SYSTEM(), map));
    StreamSpec spec = StreamSpec.createCoordinatorStreamSpec(STREAM, SYSTEM());

    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isCoordinatorStream());
      assertEquals(SYSTEM(), internalSpec.getSystemName());
      assertEquals(STREAM, internalSpec.getPhysicalName());
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
    final int PARTITIONS = 12;
    final int REP_FACTOR = 2;

    Map<String, String> map = new HashMap<>();
    map.put(JobConfig.JOB_DEFAULT_SYSTEM(), SYSTEM());
    map.put(String.format("stores.%s.changelog", "fakeStore"), topic);
    map.put(String.format("stores.%s.changelog.replication.factor", "fakeStore"), String.valueOf(REP_FACTOR));
    map.put(String.format("stores.%s.changelog.kafka.segment.bytes", "fakeStore"), "139");
    KafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(SYSTEM(), map));
    StreamSpec spec = StreamSpec.createChangeLogStreamSpec(topic, SYSTEM(), PARTITIONS);

    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isChangeLogStream());
      assertEquals(SYSTEM(), internalSpec.getSystemName());
      assertEquals(topic, internalSpec.getPhysicalName());
      assertEquals(REP_FACTOR, ((KafkaStreamSpec) internalSpec).getReplicationFactor());
      assertEquals(PARTITIONS, internalSpec.getPartitionCount());
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

    assertTrue("createStream should return true if the stream does not exist and then is created.",
        systemAdmin().createStream(spec));
    systemAdmin().validateStream(spec);

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

  //@Test //TODO - currently the connection to ZK fails, but since it checks for empty, the tests succeeds.  SAMZA-1887
  public void testClearStream() {
    StreamSpec spec = new StreamSpec("testId", "testStreamClear", "testSystem", 8);

    assertTrue("createStream should return true if the stream does not exist and then is created.",
        systemAdmin().createStream(spec));
    assertTrue(systemAdmin().clearStream(spec));

    ImmutableSet<String> topics = ImmutableSet.of(spec.getPhysicalName());
    Map<String, List<PartitionInfo>> metadata = systemAdmin().getTopicMetadata(topics);
    assertTrue(metadata.get(spec.getPhysicalName()).isEmpty());
  }
}
