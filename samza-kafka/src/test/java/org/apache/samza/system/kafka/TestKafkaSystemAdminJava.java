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
import java.util.Properties;
import java.util.function.Supplier;
import kafka.admin.AdminClient;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.KafkaConsumerConfig;
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
  private static final String KAFKA_CONSUMER_PROPERTY_PREFIX = "systems." + SYSTEM() + ".consumer.";
  private static final String KAFKA_PRODUCER_PROPERTY_PREFIX = "systems." + SYSTEM() + ".consumer.";

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

    Properties coordProps = new Properties();
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();

    SamzaKafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(coordProps, 1, changeLogMap));
    StreamSpec spec = StreamSpec.createCoordinatorStreamSpec(STREAM, SYSTEM());

    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isCoordinatorStream());
      assertEquals(SYSTEM(), internalSpec.getSystemName());
      assertEquals(STREAM, internalSpec.getPhysicalName());
      assertEquals(1, internalSpec.getPartitionCount());

      return internalSpec;
    }).when(admin).toKafkaSpec(Mockito.any());

    admin.createStream(spec);
    admin.validateStream(spec);
  }

  public SamzaKafkaSystemAdmin createSystemAdminJava(java.util.Properties coordinatorStreamProperties,
      int coordinatorStreamReplicationFactor, java.util.Map<String, ChangelogInfo> topicMetaInformation) {

    Supplier<ZkUtils> zkConnectSupplier =
        () -> ZkUtils.apply(TestKafkaSystemAdmin$.MODULE$.zkConnect(), 6000, 6000, false);

    final Properties props = new Properties();
    props.put(KafkaConsumerConfig.ZOOKEEPER_CONNECT, TestKafkaSystemAdmin$.MODULE$.zkConnect());
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafkaSystemAdmin$.MODULE$.brokerList());
    Supplier<AdminClient> adminClientSupplier = () -> AdminClient.create(props);

    Map<String, String> map = new HashMap<>();
    map.put(KAFKA_CONSUMER_PROPERTY_PREFIX + org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        TestKafkaSystemAdmin$.MODULE$.brokerList());
    map.put(JobConfig.JOB_NAME(), "job.Name");

    final Config config = new MapConfig(map);
    // extract kafka client configs
    KafkaConsumerConfig consumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, SYSTEM(), "clientPrefix");

    // KafkaConsumer for metadata access
    Supplier<Consumer<byte[], byte[]>> metadataConsumerSupplier =
        () -> KafkaSystemConsumer.getKafkaConsumerImpl(SYSTEM(), consumerConfig);

    Map<String, Properties> intermediateStreamProperties = new HashMap();
    boolean deleteCommittedMessages = false;

    return new SamzaKafkaSystemAdmin(SYSTEM(), metadataConsumerSupplier, zkConnectSupplier, adminClientSupplier,
        topicMetaInformation, intermediateStreamProperties, coordinatorStreamProperties,
        coordinatorStreamReplicationFactor, deleteCommittedMessages);
  }

  @Test
  public void testCreateChangelogStream() {
    final String STREAM = "testChangeLogStream";
    final int PARTITIONS = 12;
    final int REP_FACTOR = 1;

    Properties coordProps = new Properties();
    Properties changeLogProps = new Properties();
    changeLogProps.setProperty("cleanup.policy", "compact");
    changeLogProps.setProperty("segment.bytes", "139");
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();
    changeLogMap.put(STREAM, new ChangelogInfo(REP_FACTOR, changeLogProps));

    SamzaKafkaSystemAdmin admin = Mockito.spy(createSystemAdminJava(coordProps, 1, changeLogMap));
    StreamSpec spec = StreamSpec.createChangeLogStreamSpec(STREAM, SYSTEM(), PARTITIONS);

    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isChangeLogStream());
      assertEquals(SYSTEM(), internalSpec.getSystemName());
      assertEquals(STREAM, internalSpec.getPhysicalName());
      assertEquals(REP_FACTOR, ((KafkaStreamSpec) internalSpec).getReplicationFactor());
      assertEquals(PARTITIONS, internalSpec.getPartitionCount());
      assertEquals(changeLogProps, ((KafkaStreamSpec) internalSpec).getProperties());

      return internalSpec;
    }).when(admin).toKafkaSpec(Mockito.any());

    admin.createStream(spec);
    admin.validateStream(spec);
  }

  @Test
  public void testCreateChangelogStreamWithSpecialCharsInTopicName() {
    final String STREAM = "test.Change_Log.Stream";
    final int PARTITIONS = 12;
    final int REP_FACTOR = 1;

    Properties coordProps = new Properties();
    Properties changeLogProps = new Properties();
    changeLogProps.setProperty("cleanup.policy", "compact");
    changeLogProps.setProperty("segment.bytes", "139");
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();
    changeLogMap.put(STREAM, new ChangelogInfo(REP_FACTOR, changeLogProps));

    SamzaKafkaSystemAdmin admin = Mockito.spy(createSystemAdminJava(coordProps, 1, changeLogMap));
    StreamSpec spec = StreamSpec.createChangeLogStreamSpec(STREAM, SYSTEM(), PARTITIONS);
    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isChangeLogStream());
      assertEquals(SYSTEM(), internalSpec.getSystemName());
      assertEquals(STREAM, internalSpec.getPhysicalName());
      assertEquals(REP_FACTOR, ((KafkaStreamSpec) internalSpec).getReplicationFactor());
      assertEquals(PARTITIONS, internalSpec.getPartitionCount());
      assertEquals(changeLogProps, ((KafkaStreamSpec) internalSpec).getProperties());

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

  //@Test TODO - see if it can be fixed
  public void testClearStream() {
    StreamSpec spec = new StreamSpec("testId", "testStreamClear", "testSystem", 8);

    assertTrue("createStream should return true if the stream does not exist and then is created.",
        systemAdmin().createStream(spec));
    assertTrue(systemAdmin().clearStream(spec));

    ImmutableSet<String> topics = ImmutableSet.of(spec.getPhysicalName());
    Map<String, List<PartitionInfo>> metadata = systemAdmin().getTopicMetadata(topics);
    assertTrue(metadata.get(spec.getPhysicalName()).isEmpty());

    //scala.collection.immutable.Set<String> topics = new scala.collection.immutable.Set.Set1<>(spec.getPhysicalName());
    //scala.collection.immutable.Map<String, TopicMetadata> metadata = ((KafkaSystemAdmin)systemAdmin()).getTopicMetadata(topics);
    //assertTrue(metadata.get(spec.getPhysicalName()).get().partitionsMetadata().isEmpty());

  }
}
