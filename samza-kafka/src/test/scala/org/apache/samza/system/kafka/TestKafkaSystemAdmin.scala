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

import java.util.{Properties, UUID}

import kafka.admin.AdminUtils
import kafka.common.{ErrorMapping, LeaderNotAvailableException}
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, ZkUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.security.JaasUtils
import org.apache.samza.Partition
import org.apache.samza.config.KafkaProducerConfig
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.{StreamSpec, SystemStreamMetadata, SystemStreamPartition}
import org.apache.samza.util.{ClientUtilTopicMetadataStore, ExponentialSleepStrategy, KafkaUtil, TopicMetadataStore}
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConverters._

/**
  * README: New tests should be added to the Java tests. See TestKafkaSystemAdminJava
  */
object TestKafkaSystemAdmin extends KafkaServerTestHarness {

  val SYSTEM = "kafka"
  val TOPIC = "input"
  val TOPIC2 = "input2"
  val TOTAL_PARTITIONS = 50
  val REPLICATION_FACTOR = 2
  val zkSecure = JaasUtils.isZkSecurityEnabled()

  protected def numBrokers: Int = 3

  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  var metadataStore: TopicMetadataStore = null
  var producerConfig: KafkaProducerConfig = null
  var brokers: String = null

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, true)
    props.map(KafkaConfig.fromProps)
  }

  @BeforeClass
  override def setUp {
    super.setUp

    val config = new java.util.HashMap[String, String]()

    brokers = brokerList.split(",").map(p => "localhost" + p).mkString(",")

    config.put("bootstrap.servers", brokers)
    config.put("acks", "all")
    config.put("serializer.class", "kafka.serializer.StringEncoder")

    producerConfig = new KafkaProducerConfig("kafka", "i001", config)

    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")
  }


  @AfterClass
  override def tearDown {
    super.tearDown
  }


  def createTopic(topicName: String, partitionCount: Int) {
    AdminUtils.createTopic(
      zkUtils,
      topicName,
      partitionCount,
      REPLICATION_FACTOR)
  }

  def validateTopic(topic: String, expectedPartitionCount: Int) {
    var done = false
    var retries = 0
    val maxRetries = 100

    while (!done && retries < maxRetries) {
      try {
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(topic), SYSTEM, metadataStore.getTopicInfo)
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

  def createSystemAdmin: KafkaSystemAdmin = {
    new KafkaSystemAdmin(SYSTEM, brokers, connectZk = () => ZkUtils(zkConnect, 6000, 6000, zkSecure))
  }

  def createSystemAdmin(coordinatorStreamProperties: Properties, coordinatorStreamReplicationFactor: Int, topicMetaInformation: Map[String, ChangelogInfo]): KafkaSystemAdmin = {
    new KafkaSystemAdmin(SYSTEM, brokers, connectZk = () => ZkUtils(zkConnect, 6000, 6000, zkSecure), coordinatorStreamProperties, coordinatorStreamReplicationFactor, 10000, ConsumerConfig.SocketBufferSize, UUID.randomUUID.toString, topicMetaInformation, Map())
  }

}

/**
 * Test creates a local ZK and Kafka cluster, and uses it to create and test
 * topics for to verify that offset APIs in SystemAdmin work as expected.
 */
class TestKafkaSystemAdmin {
  import TestKafkaSystemAdmin._

  // Provide a random zkAddress, the system admin tries to connect only when a topic is created/validated
  val systemAdmin = createSystemAdmin

  @Test
  def testShouldAssembleMetadata {
    val oldestOffsets = Map(
      new SystemStreamPartition(SYSTEM, "stream1", new Partition(0)) -> "o1",
      new SystemStreamPartition(SYSTEM, "stream2", new Partition(0)) -> "o2",
      new SystemStreamPartition(SYSTEM, "stream1", new Partition(1)) -> "o3",
      new SystemStreamPartition(SYSTEM, "stream2", new Partition(1)) -> "o4")
    val newestOffsets = Map(
      new SystemStreamPartition(SYSTEM, "stream1", new Partition(0)) -> "n1",
      new SystemStreamPartition(SYSTEM, "stream2", new Partition(0)) -> "n2",
      new SystemStreamPartition(SYSTEM, "stream1", new Partition(1)) -> "n3",
      new SystemStreamPartition(SYSTEM, "stream2", new Partition(1)) -> "n4")
    val upcomingOffsets = Map(
      new SystemStreamPartition(SYSTEM, "stream1", new Partition(0)) -> "u1",
      new SystemStreamPartition(SYSTEM, "stream2", new Partition(0)) -> "u2",
      new SystemStreamPartition(SYSTEM, "stream1", new Partition(1)) -> "u3",
      new SystemStreamPartition(SYSTEM, "stream2", new Partition(1)) -> "u4")
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
    createTopic(TOPIC, 50)
    validateTopic(TOPIC, 50)

    // Verify the empty topic behaves as expected.
    var metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC).asJava)
    assertEquals(1, metadata.size)
    assertNotNull(metadata.get(TOPIC))
    // Verify partition count.
    var sspMetadata = metadata.get(TOPIC).getSystemStreamPartitionMetadata
    assertEquals(50, sspMetadata.size)
    // Empty topics should have null for latest offset and 0 for earliest offset
    assertEquals("0", sspMetadata.get(new Partition(0)).getOldestOffset)
    assertNull(sspMetadata.get(new Partition(0)).getNewestOffset)
    // Empty Kafka topics should have a next offset of 0.
    assertEquals("0", sspMetadata.get(new Partition(0)).getUpcomingOffset)

    // Add a new message to one of the partitions, and verify that it works as
    // expected.
    producer.send(new ProducerRecord(TOPIC, 48, "key1".getBytes, "val1".getBytes)).get()
    metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC).asJava)
    assertEquals(1, metadata.size)
    val streamName = metadata.keySet.asScala.head
    assertEquals(TOPIC, streamName)
    sspMetadata = metadata.get(streamName).getSystemStreamPartitionMetadata
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
    metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC).asJava)
    assertEquals(1, metadata.size)
    assertEquals(TOPIC, streamName)
    sspMetadata = metadata.get(streamName).getSystemStreamPartitionMetadata
    // key1 gets hash-mod'd to partition 48.
    assertEquals("0", sspMetadata.get(new Partition(48)).getOldestOffset)
    assertEquals("1", sspMetadata.get(new Partition(48)).getNewestOffset)
    assertEquals("2", sspMetadata.get(new Partition(48)).getUpcomingOffset)

    // Validate that a fetch will return the message.
    val connector = getConsumerConnector
    var stream = connector.createMessageStreams(Map(TOPIC -> 1))(TOPIC).head.iterator
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
    val initialOffsets = systemAdmin.getSystemStreamMetadata(Set("non-existent-topic").asJava)
    val metadata = initialOffsets.asScala.getOrElse("non-existent-topic", fail("missing metadata"))
    assertEquals(metadata, new SystemStreamMetadata("non-existent-topic", Map(
      new Partition(0) -> new SystemStreamPartitionMetadata("0", null, "0")).asJava))
  }

  @Test
  def testOffsetsAfter {
    val ssp1 = new SystemStreamPartition("test-system", "test-stream", new Partition(0))
    val ssp2 = new SystemStreamPartition("test-system", "test-stream", new Partition(1))
    val offsetsAfter = systemAdmin.getOffsetsAfter(Map(
      ssp1 -> "1",
      ssp2 -> "2").asJava)
    assertEquals("2", offsetsAfter.get(ssp1))
    assertEquals("3", offsetsAfter.get(ssp2))
  }

  @Test
  def testShouldCreateCoordinatorStream {
    val topic = "test-coordinator-stream"
    val systemAdmin = new KafkaSystemAdmin(SYSTEM, brokers, () => ZkUtils(zkConnect, 6000, 6000, zkSecure), coordinatorStreamReplicationFactor = 3)

    val spec = StreamSpec.createCoordinatorStreamSpec(topic, "kafka")
    systemAdmin.createStream(spec)
    validateTopic(topic, 1)
    val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(topic), "kafka", metadataStore.getTopicInfo)
    assertTrue(topicMetadataMap.contains(topic))
    val topicMetadata = topicMetadataMap(topic)
    val partitionMetadata = topicMetadata.partitionsMetadata.head
    assertEquals(0, partitionMetadata.partitionId)
    assertEquals(3, partitionMetadata.replicas.size)
  }

  class KafkaSystemAdminWithTopicMetadataError extends KafkaSystemAdmin(SYSTEM, brokers, () => ZkUtils(zkConnect, 6000, 6000, zkSecure)) {
    import kafka.api.TopicMetadata
    var metadataCallCount = 0

    // Simulate Kafka telling us that the leader for the topic is not available
    override def getTopicMetadata(topics: Set[String]) = {
      metadataCallCount += 1
      val topicMetadata = TopicMetadata(topic = "quux", partitionsMetadata = Seq(), errorCode = ErrorMapping.LeaderNotAvailableCode)
      Map("quux" -> topicMetadata)
    }
  }

  @Test
  def testShouldRetryOnTopicMetadataError {
    val systemAdmin = new KafkaSystemAdminWithTopicMetadataError
    val retryBackoff = new ExponentialSleepStrategy.Mock(maxCalls = 3)
    try {
      systemAdmin.getSystemStreamMetadata(Set("quux").asJava, retryBackoff)
      fail("expected CallLimitReached to be thrown")
    } catch {
      case e: ExponentialSleepStrategy.CallLimitReached => ()
    }
  }

  @Test
  def testGetNewestOffset {
    createTopic(TOPIC2, 16)
    validateTopic(TOPIC2, 16)

    val sspUnderTest = new SystemStreamPartition("kafka", TOPIC2, new Partition(4))
    val otherSsp = new SystemStreamPartition("kafka", TOPIC2, new Partition(13))

    assertNull(systemAdmin.getNewestOffset(sspUnderTest, 3))
    assertNull(systemAdmin.getNewestOffset(otherSsp, 3))

    // Add a new message to one of the partitions, and verify that it works as expected.
    assertEquals("0", producer.send(new ProducerRecord(TOPIC2, 4, "key1".getBytes, "val1".getBytes)).get().offset().toString)
    assertEquals("0", systemAdmin.getNewestOffset(sspUnderTest, 3))
    assertNull(systemAdmin.getNewestOffset(otherSsp, 3))

    // Again
    assertEquals("1", producer.send(new ProducerRecord(TOPIC2, 4, "key2".getBytes, "val2".getBytes)).get().offset().toString)
    assertEquals("1", systemAdmin.getNewestOffset(sspUnderTest, 3))
    assertNull(systemAdmin.getNewestOffset(otherSsp, 3))

    // Add a message to both partitions
    assertEquals("2", producer.send(new ProducerRecord(TOPIC2, 4, "key3".getBytes, "val3".getBytes)).get().offset().toString)
    assertEquals("0", producer.send(new ProducerRecord(TOPIC2, 13, "key4".getBytes, "val4".getBytes)).get().offset().toString)
    assertEquals("2", systemAdmin.getNewestOffset(sspUnderTest, 0))
    assertEquals("0", systemAdmin.getNewestOffset(otherSsp, 0))
  }

  @Test (expected = classOf[LeaderNotAvailableException])
  def testGetNewestOffsetMaxRetry {
    val expectedRetryCount = 3
    val systemAdmin = new KafkaSystemAdminWithTopicMetadataError
    try {
      systemAdmin.getNewestOffset(new SystemStreamPartition(SYSTEM, "quux", new Partition(0)), 3)
    } catch {
      case e: Exception =>
        assertEquals(expectedRetryCount + 1, systemAdmin.metadataCallCount)
        throw e
    }
  }
}
