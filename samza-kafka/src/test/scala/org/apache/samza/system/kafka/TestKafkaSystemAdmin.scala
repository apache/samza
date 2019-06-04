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

import java.time.Duration
import java.util.Collections
import java.util.concurrent.TimeUnit

import com.google.common.collect.ImmutableSet
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.{KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.security.JaasUtils
import org.apache.samza.Partition
import org.apache.samza.config._
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.{StreamSpec, SystemStreamMetadata, SystemStreamPartition}
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConverters
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
  val KAFKA_CONSUMER_PROPERTY_PREFIX: String = "systems." + SYSTEM + ".consumer."
  val KAFKA_PRODUCER_PROPERTY_PREFIX: String = "systems." + SYSTEM + ".producer."


  protected def numBrokers: Int = 3

  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  var producerConfig: KafkaProducerConfig = null
  var systemAdmin: KafkaSystemAdmin = null
  var adminClient: AdminClient = null

  override def generateConfigs(): Seq[KafkaConfig] = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, enableDeleteTopic = true)
    props.map(KafkaConfig.fromProps)
  }

  @BeforeClass
  override def setUp() {
    super.setUp()
    val map = new java.util.HashMap[String, String]()
    map.put("bootstrap.servers", brokerList)
    map.put(KAFKA_CONSUMER_PROPERTY_PREFIX +
      org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    map.put("acks", "all")
    map.put("serializer.class", "kafka.serializer.StringEncoder")


    producerConfig = new KafkaProducerConfig("kafka", "i001", map)
    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    systemAdmin = createSystemAdmin(SYSTEM, map)
    adminClient = AdminClient.create(map.asInstanceOf[java.util.Map[String, Object]])

    systemAdmin.start()
  }

  @AfterClass
  override def tearDown() {
    adminClient.close()
    systemAdmin.stop()
    producer.close()
    super.tearDown()
  }

  def createTopic(topicName: String, partitionCount: Int) {
    val topicCreationFutures = adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, partitionCount, REPLICATION_FACTOR.shortValue())))
    topicCreationFutures.all().get() // wait on the future to make sure create topic call finishes
  }

  def validateTopic(topic: String, expectedPartitionCount: Int) {
    var done = false
    var attempts = 0

    while (!done && attempts < 10) {
      try {
        attempts += 1
        val topicDescriptionFutures = adminClient.describeTopics(
            JavaConverters.asJavaCollectionConverter(Set(topic)).asJavaCollection).all()
        val topicDescription = topicDescriptionFutures.get(500, TimeUnit.MILLISECONDS)
            .get(topic)

        done = expectedPartitionCount == topicDescription.partitions().size()
      } catch {
        case e: Exception =>
          System.err.println("Interrupted during validating test topics", e)
      }
    }

    if (!done) {
      fail("Unable to successfully create topics. Tried to validate %s times." format attempts)
    }
  }

  def getKafkaConsumer(): KafkaConsumer[Any, Any] = {
    val props = new java.util.Properties

    props.put("bootstrap.servers", brokerList)
    props.put("group.id", "test")
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    new KafkaConsumer[Any, Any](props)
  }

  def createSystemAdmin(system: String, map: java.util.Map[String, String]) = {
    // required configs - boostraplist, zkconnect and jobname
    map.put(KAFKA_CONSUMER_PROPERTY_PREFIX + org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      brokerList)
    map.put(KAFKA_PRODUCER_PROPERTY_PREFIX + org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      brokerList)
    map.put(JobConfig.JOB_NAME, "job.name")
    map.put(KAFKA_CONSUMER_PROPERTY_PREFIX + KafkaConsumerConfig.ZOOKEEPER_CONNECT, zkConnect)

    val config: Config = new MapConfig(map)

    val clientId = KafkaConsumerConfig.createClientId("clientPrefix", config)
    // extract kafka client configs
    val consumerConfig = KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, system, clientId)

    new KafkaSystemAdmin(system, config, KafkaSystemConsumer.createKafkaConsumerImpl(system, consumerConfig))
  }
}

/**
  * Test creates a local ZK and Kafka cluster, and uses it to create and test
  * topics for to verify that offset APIs in SystemAdmin work as expected.
  */
class TestKafkaSystemAdmin {

  import TestKafkaSystemAdmin._

  @Test
  def testShouldGetOldestNewestAndNextOffsets {
    // Reduced the partition count to 25 instead of 50 to prevent tests from failing. In the current setup,
    // embedded Kafka brings up 3 brokers for test and replication fetcher runs into issue with higher number partition.
    // Create an empty topic with 25 partitions, but with no offsets.
    createTopic(TOPIC, 25)
    validateTopic(TOPIC, 25)

    // Verify the empty topic behaves as expected.
    var metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC).asJava)
    assertEquals(1, metadata.size)
    assertNotNull(metadata.get(TOPIC))
    // Verify partition count.
    var sspMetadata = metadata.get(TOPIC).getSystemStreamPartitionMetadata
    assertEquals(25, sspMetadata.size)
    // Empty topics should have null for latest offset and 0 for earliest offset
    assertEquals("0", sspMetadata.get(new Partition(0)).getOldestOffset)
    assertNull(sspMetadata.get(new Partition(0)).getNewestOffset)
    // Empty Kafka topics should have a next offset of 0.
    assertEquals("0", sspMetadata.get(new Partition(0)).getUpcomingOffset)

    // Add a new message to one of the partitions, and verify that it works as
    // expected.
    producer.send(new ProducerRecord(TOPIC, 24, "key1".getBytes, "val1".getBytes)).get()
    metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC).asJava)
    assertEquals(1, metadata.size)
    val streamName = metadata.keySet.asScala.head
    assertEquals(TOPIC, streamName)
    sspMetadata = metadata.get(streamName).getSystemStreamPartitionMetadata
    // key1 gets hash-mod'd to partition 48.
    assertEquals("0", sspMetadata.get(new Partition(24)).getOldestOffset)
    assertEquals("0", sspMetadata.get(new Partition(24)).getNewestOffset)
    assertEquals("1", sspMetadata.get(new Partition(24)).getUpcomingOffset)
    // Some other partition should be empty.
    assertEquals("0", sspMetadata.get(new Partition(3)).getOldestOffset)
    assertNull(sspMetadata.get(new Partition(3)).getNewestOffset)
    assertEquals("0", sspMetadata.get(new Partition(3)).getUpcomingOffset)

    // Add a second message to one of the same partition.
    producer.send(new ProducerRecord(TOPIC, 24, "key1".getBytes, "val2".getBytes)).get()
    metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC).asJava)
    assertEquals(1, metadata.size)
    assertEquals(TOPIC, streamName)
    sspMetadata = metadata.get(streamName).getSystemStreamPartitionMetadata
    // key1 gets hash-mod'd to partition 48.
    assertEquals("0", sspMetadata.get(new Partition(24)).getOldestOffset)
    assertEquals("1", sspMetadata.get(new Partition(24)).getNewestOffset)
    assertEquals("2", sspMetadata.get(new Partition(24)).getUpcomingOffset)

    // Validate that a fetch will return the message.
    val consumer = getKafkaConsumer
    consumer.subscribe(JavaConverters.setAsJavaSetConverter(Set(TOPIC)).asJava)
    val records = consumer.poll(Duration.ofMillis(10000))
    consumer.close()

    assertTrue(records.count() == 2)
    val stream = records.iterator()
    var message = stream.next()

    var text = new String(message.value().asInstanceOf[Array[Byte]], "UTF-8")
    // First message should match the earliest expected offset.
    assertEquals(sspMetadata.get(new Partition(24)).getOldestOffset, message.offset.toString)
    assertEquals("val1", text)
    // Second message should match the earliest expected offset.
    message = stream.next()
    text = new String(message.value().asInstanceOf[Array[Byte]], "UTF-8")
    assertEquals(sspMetadata.get(new Partition(24)).getNewestOffset, message.offset.toString)
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
    val expectedReplicationFactor = 3
    val map = new java.util.HashMap[String, String]()
    map.put(org.apache.samza.config.KafkaConfig.JOB_COORDINATOR_REPLICATION_FACTOR, expectedReplicationFactor.toString)
    val systemAdmin = createSystemAdmin(SYSTEM, map)

    val spec = StreamSpec.createCoordinatorStreamSpec(topic, "kafka")
    val actualReplicationFactor = systemAdmin.toKafkaSpec(spec).getReplicationFactor
    // Ensure we respect the replication factor passed to system admin
    assertEquals(expectedReplicationFactor, actualReplicationFactor)

    systemAdmin.createStream(spec)
    validateTopic(topic, 1)
  }

  @Test
  def testGetNewestOffset {
    createTopic(TOPIC2, 16)
    validateTopic(TOPIC2, 16)

    val sspUnderTest = new SystemStreamPartition("kafka", TOPIC2, new Partition(4))
    val otherSsp = new SystemStreamPartition("kafka", TOPIC2, new Partition(13))

    assertNull(systemAdmin.getSSPMetadata(ImmutableSet.of(sspUnderTest)).get(sspUnderTest).getNewestOffset)
    assertNull(systemAdmin.getSSPMetadata(ImmutableSet.of(otherSsp)).get(otherSsp).getNewestOffset)

    // Add a new message to one of the partitions, and verify that it works as expected.
    assertEquals("0", producer.send(new ProducerRecord(TOPIC2, 4, "key1".getBytes, "val1".getBytes)).get().offset().toString)

    assertEquals("0", systemAdmin.getSSPMetadata(ImmutableSet.of(sspUnderTest)).get(sspUnderTest).getNewestOffset)
    assertNull(systemAdmin.getSSPMetadata(ImmutableSet.of(otherSsp)).get(otherSsp).getNewestOffset)

    // Again
    assertEquals("1", producer.send(new ProducerRecord(TOPIC2, 4, "key2".getBytes, "val2".getBytes)).get().offset().toString)
    assertEquals("1", systemAdmin.getSSPMetadata(ImmutableSet.of(sspUnderTest)).get(sspUnderTest).getNewestOffset)
    assertNull(systemAdmin.getSSPMetadata(ImmutableSet.of(otherSsp)).get(otherSsp).getNewestOffset)

    // Add a message to both partitions
    assertEquals("2", producer.send(new ProducerRecord(TOPIC2, 4, "key3".getBytes, "val3".getBytes)).get().offset().toString)
    assertEquals("0", producer.send(new ProducerRecord(TOPIC2, 13, "key4".getBytes, "val4".getBytes)).get().offset().toString)
    assertEquals("2", systemAdmin.getSSPMetadata(ImmutableSet.of(sspUnderTest)).get(sspUnderTest).getNewestOffset)
    assertEquals("0", systemAdmin.getSSPMetadata(ImmutableSet.of(otherSsp)).get(otherSsp).getNewestOffset)
  }
}