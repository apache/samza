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

package org.apache.samza.system.kafka

import java.util.Properties
import kafka.admin.AdminUtils
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.utils.TestZKUtils
import kafka.utils.Utils
import kafka.utils.ZKStringSerializer
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient
import org.apache.samza.Partition
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.ExponentialSleepStrategy
import org.apache.samza.util.ClientUtilTopicMetadataStore
import org.apache.samza.util.TopicMetadataStore
import org.junit.Assert._
import org.junit.{Test, BeforeClass, AfterClass}
import scala.collection.JavaConversions._
import org.apache.samza.config.KafkaProducerConfig
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import java.util
import kafka.common.ErrorMapping
import org.apache.samza.util.KafkaUtil

object TestKafkaSystemAdmin {
  val TOPIC = "input"
  val TOTAL_PARTITIONS = 50
  val REPLICATION_FACTOR = 2

  val zkConnect: String = TestZKUtils.zookeeperConnect
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000
  val brokerId1 = 0
  val brokerId2 = 1
  val brokerId3 = 2
  val ports = TestUtils.choosePorts(3)
  val (port1, port2, port3) = (ports(0), ports(1), ports(2))

  val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
  val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
  val props3 = TestUtils.createBrokerConfig(brokerId3, port3)

  val config = new util.HashMap[String, Object]()
  val brokers = "localhost:%d,localhost:%d,localhost:%d" format (port1, port2, port3)
  config.put("bootstrap.servers", brokers)
  config.put("request.required.acks", "-1")
  config.put("serializer.class", "kafka.serializer.StringEncoder")
  val producerConfig = new KafkaProducerConfig("kafka", "i001", config)
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  var zookeeper: EmbeddedZookeeper = null
  var server1: KafkaServer = null
  var server2: KafkaServer = null
  var server3: KafkaServer = null
  var metadataStore: TopicMetadataStore = null

  @BeforeClass
  def beforeSetupServers {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    server1 = TestUtils.createServer(new KafkaConfig(props1))
    server2 = TestUtils.createServer(new KafkaConfig(props2))
    server3 = TestUtils.createServer(new KafkaConfig(props3))
    zkClient = new ZkClient(zkConnect + "/", 6000, 6000, ZKStringSerializer)
    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")
  }

  def createTopic {
    AdminUtils.createTopic(
      zkClient,
      TOPIC,
      TOTAL_PARTITIONS,
      REPLICATION_FACTOR)
  }

  def validateTopic(topic: String, expectedPartitionCount: Int) {
    var done = false
    var retries = 0
    val maxRetries = 100

    while (!done && retries < maxRetries) {
      try {
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(topic), "kafka", metadataStore.getTopicInfo)
        val topicMetadata = topicMetadataMap(topic)
        val errorCode = topicMetadata.errorCode

        KafkaUtil.maybeThrowException(errorCode)

        done = expectedPartitionCount == topicMetadata.partitionsMetadata.size
      } catch {
        case e: Exception =>
          System.err.println("Got exception while validating test topics. Waiting and retrying.", e)
          retries += 1
          Thread.sleep(500)
      }
    }

    if (retries >= maxRetries) {
      fail("Unable to successfully create topics. Tried to validate %s times." format retries)
    }
  }

  def getConsumerConnector = {
    val props = new Properties

    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", "test")
    props.put("auto.offset.reset", "smallest")

    val consumerConfig = new ConsumerConfig(props)
    Consumer.create(consumerConfig)
  }

  @AfterClass
  def afterCleanLogDirs {
    producer.close()
    server1.shutdown
    server1.awaitShutdown()
    server2.shutdown
    server2.awaitShutdown()
    server3.shutdown
    server3.awaitShutdown()
    Utils.rm(server1.config.logDirs)
    Utils.rm(server2.config.logDirs)
    Utils.rm(server3.config.logDirs)
    zkClient.close
    zookeeper.shutdown
  }
}

/**
 * Test creates a local ZK and Kafka cluster, and uses it to create and test
 * topics for to verify that offset APIs in SystemAdmin work as expected.
 */
class TestKafkaSystemAdmin {
  import TestKafkaSystemAdmin._

  val systemName = "test"
  // Provide a random zkAddress, the system admin tries to connect only when a topic is created/validated
  val systemAdmin = new KafkaSystemAdmin(systemName, brokers, connectZk = () => new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer))

  def testShouldAssembleMetadata {
    val oldestOffsets = Map(
      new SystemStreamPartition("test", "stream1", new Partition(0)) -> "o1",
      new SystemStreamPartition("test", "stream2", new Partition(0)) -> "o2",
      new SystemStreamPartition("test", "stream1", new Partition(1)) -> "o3",
      new SystemStreamPartition("test", "stream2", new Partition(1)) -> "o4")
    val newestOffsets = Map(
      new SystemStreamPartition("test", "stream1", new Partition(0)) -> "n1",
      new SystemStreamPartition("test", "stream2", new Partition(0)) -> "n2",
      new SystemStreamPartition("test", "stream1", new Partition(1)) -> "n3",
      new SystemStreamPartition("test", "stream2", new Partition(1)) -> "n4")
    val upcomingOffsets = Map(
      new SystemStreamPartition("test", "stream1", new Partition(0)) -> "u1",
      new SystemStreamPartition("test", "stream2", new Partition(0)) -> "u2",
      new SystemStreamPartition("test", "stream1", new Partition(1)) -> "u3",
      new SystemStreamPartition("test", "stream2", new Partition(1)) -> "u4")
    val metadata = KafkaSystemAdmin.assembleMetadata(oldestOffsets, newestOffsets, upcomingOffsets)
    assertNotNull(metadata)
    assertEquals(2, metadata.size)
    assertTrue(metadata.contains("stream1"))
    assertTrue(metadata.contains("stream2"))
    val stream1Metadata = metadata("stream1")
    val stream2Metadata = metadata("stream2")
    assertNotNull(stream1Metadata)
    assertNotNull(stream2Metadata)
    assertEquals("stream1", stream1Metadata.getStreamName)
    assertEquals("stream2", stream2Metadata.getStreamName)
    val expectedSystemStream1Partition0Metadata = new SystemStreamPartitionMetadata("o1", "n1", "u1")
    val expectedSystemStream1Partition1Metadata = new SystemStreamPartitionMetadata("o3", "n3", "u3")
    val expectedSystemStream2Partition0Metadata = new SystemStreamPartitionMetadata("o2", "n2", "u2")
    val expectedSystemStream2Partition1Metadata = new SystemStreamPartitionMetadata("o4", "n4", "u4")
    val stream1PartitionMetadata = stream1Metadata.getSystemStreamPartitionMetadata
    val stream2PartitionMetadata = stream2Metadata.getSystemStreamPartitionMetadata
    assertEquals(expectedSystemStream1Partition0Metadata, stream1PartitionMetadata.get(new Partition(0)))
    assertEquals(expectedSystemStream1Partition1Metadata, stream1PartitionMetadata.get(new Partition(1)))
    assertEquals(expectedSystemStream2Partition0Metadata, stream2PartitionMetadata.get(new Partition(0)))
    assertEquals(expectedSystemStream2Partition1Metadata, stream2PartitionMetadata.get(new Partition(1)))
  }

  @Test
  def testShouldGetOldestNewestAndNextOffsets {
    // Create an empty topic with 50 partitions, but with no offsets.
    createTopic
    validateTopic(TOPIC, 50)

    // Verify the empty topic behaves as expected.
    var metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC))
    assertEquals(1, metadata.size)
    assertNotNull(metadata(TOPIC))
    // Verify partition count.
    var sspMetadata = metadata(TOPIC).getSystemStreamPartitionMetadata
    assertEquals(50, sspMetadata.size)
    // Empty topics should have null for latest offset and 0 for earliest offset
    assertEquals("0", sspMetadata.get(new Partition(0)).getOldestOffset)
    assertNull(sspMetadata.get(new Partition(0)).getNewestOffset)
    // Empty Kafka topics should have a next offset of 0.
    assertEquals("0", sspMetadata.get(new Partition(0)).getUpcomingOffset)

    // Add a new message to one of the partitions, and verify that it works as 
    // expected.
    producer.send(new ProducerRecord(TOPIC, 48, "key1".getBytes, "val1".getBytes)).get()
    metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC))
    assertEquals(1, metadata.size)
    val streamName = metadata.keySet.head
    assertEquals(TOPIC, streamName)
    sspMetadata = metadata(streamName).getSystemStreamPartitionMetadata
    // key1 gets hash-mod'd to partition 48.
    assertEquals("0", sspMetadata.get(new Partition(48)).getOldestOffset)
    assertEquals("0", sspMetadata.get(new Partition(48)).getNewestOffset)
    assertEquals("1", sspMetadata.get(new Partition(48)).getUpcomingOffset)
    // Some other partition should be empty.
    assertEquals("0", sspMetadata.get(new Partition(3)).getOldestOffset)
    assertNull(sspMetadata.get(new Partition(3)).getNewestOffset)
    assertEquals("0", sspMetadata.get(new Partition(3)).getUpcomingOffset)

    // Add a second message to one of the same partition.
    producer.send(new ProducerRecord(TOPIC, 48, "key1".getBytes, "val2".getBytes)).get()
    metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC))
    assertEquals(1, metadata.size)
    assertEquals(TOPIC, streamName)
    sspMetadata = metadata(streamName).getSystemStreamPartitionMetadata
    // key1 gets hash-mod'd to partition 48.
    assertEquals("0", sspMetadata.get(new Partition(48)).getOldestOffset)
    assertEquals("1", sspMetadata.get(new Partition(48)).getNewestOffset)
    assertEquals("2", sspMetadata.get(new Partition(48)).getUpcomingOffset)

    // Validate that a fetch will return the message.
    val connector = getConsumerConnector
    var stream = connector.createMessageStreams(Map(TOPIC -> 1)).get(TOPIC).get.get(0).iterator
    var message = stream.next
    var text = new String(message.message, "UTF-8")
    connector.shutdown
    // First message should match the earliest expected offset.
    assertEquals(sspMetadata.get(new Partition(48)).getOldestOffset, message.offset.toString)
    assertEquals("val1", text)
    // Second message should match the earliest expected offset.
    message = stream.next
    text = new String(message.message, "UTF-8")
    assertEquals(sspMetadata.get(new Partition(48)).getNewestOffset, message.offset.toString)
    assertEquals("val2", text)
  }

  @Test
  def testNonExistentTopic {
    val initialOffsets = systemAdmin.getSystemStreamMetadata(Set("non-existent-topic"))
    val metadata = initialOffsets.getOrElse("non-existent-topic", fail("missing metadata"))
    assertEquals(metadata, new SystemStreamMetadata("non-existent-topic", Map(
      new Partition(0) -> new SystemStreamPartitionMetadata("0", null, "0"))))
  }

  @Test
  def testOffsetsAfter {
    val ssp1 = new SystemStreamPartition("test-system", "test-stream", new Partition(0))
    val ssp2 = new SystemStreamPartition("test-system", "test-stream", new Partition(1))
    val offsetsAfter = systemAdmin.getOffsetsAfter(Map(
      ssp1 -> "1",
      ssp2 -> "2"))
    assertEquals("2", offsetsAfter(ssp1))
    assertEquals("3", offsetsAfter(ssp2))
  }

  @Test
  def testShouldCreateCoordinatorStream {
    val topic = "test-coordinator-stream"
    val systemAdmin = new KafkaSystemAdmin("test", brokers, () => new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer), coordinatorStreamReplicationFactor = 3)
    systemAdmin.createCoordinatorStream(topic)
    validateTopic(topic, 1)
    val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(topic), "kafka", metadataStore.getTopicInfo)
    assertTrue(topicMetadataMap.contains(topic))
    val topicMetadata = topicMetadataMap(topic)
    val partitionMetadata = topicMetadata.partitionsMetadata.head
    assertEquals(0, partitionMetadata.partitionId)
    assertEquals(3, partitionMetadata.replicas.size)
  }

  class KafkaSystemAdminWithTopicMetadataError extends KafkaSystemAdmin("test", brokers, () => new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer)) {
    import kafka.api.{ TopicMetadata, TopicMetadataResponse }

    // Simulate Kafka telling us that the leader for the topic is not available
    override def getTopicMetadata(topics: Set[String]) = {
      val topicMetadata = TopicMetadata(topic = "quux", partitionsMetadata = Seq(), errorCode = ErrorMapping.LeaderNotAvailableCode)
      Map("quux" -> topicMetadata)
    }
  }

  @Test
  def testShouldRetryOnTopicMetadataError {
    val systemAdmin = new KafkaSystemAdminWithTopicMetadataError
    val retryBackoff = new ExponentialSleepStrategy.Mock(maxCalls = 3)
    try {
      systemAdmin.getSystemStreamMetadata(Set("quux"), retryBackoff)
      fail("expected CallLimitReached to be thrown")
    } catch {
      case e: ExponentialSleepStrategy.CallLimitReached => ()
    }
  }
}
