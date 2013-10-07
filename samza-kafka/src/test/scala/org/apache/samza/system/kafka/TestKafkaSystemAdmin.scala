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

import org.junit.Assert._
import org.junit.Test
import kafka.zk.EmbeddedZookeeper
import org.junit.BeforeClass
import org.junit.AfterClass
import org.apache.samza.util.ClientUtilTopicMetadataStore
import org.I0Itec.zkclient.ZkClient
import kafka.admin.AdminUtils
import org.apache.samza.util.TopicMetadataStore
import kafka.producer.ProducerConfig
import kafka.utils.TestUtils
import kafka.common.ErrorMapping
import kafka.utils.TestZKUtils
import kafka.server.KafkaServer
import kafka.producer.Producer
import kafka.server.KafkaConfig
import kafka.utils.Utils
import kafka.utils.ZKStringSerializer
import scala.collection.JavaConversions._
import kafka.producer.KeyedMessage
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import java.util.Properties

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
  val config1 = new KafkaConfig(props1) {
    override val hostName = "localhost"
    override val numPartitions = 1
    override val zkConnect = TestKafkaSystemAdmin.zkConnect + "/"
  }
  val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
  val config2 = new KafkaConfig(props2) {
    override val hostName = "localhost"
    override val numPartitions = 1
    override val zkConnect = TestKafkaSystemAdmin.zkConnect + "/"
  }
  val props3 = TestUtils.createBrokerConfig(brokerId3, port3)
  val config3 = new KafkaConfig(props3) {
    override val hostName = "localhost"
    override val numPartitions = 1
    override val zkConnect = TestKafkaSystemAdmin.zkConnect + "/"
  }

  val config = new java.util.Properties()
  val brokers = "localhost:%d,localhost:%d,localhost:%d" format (port1, port2, port3)
  config.put("metadata.broker.list", brokers)
  config.put("producer.type", "sync")
  config.put("request.required.acks", "-1")
  config.put("serializer.class", "kafka.serializer.StringEncoder");
  val producerConfig = new ProducerConfig(config)
  var producer: Producer[String, String] = null
  var zookeeper: EmbeddedZookeeper = null
  var server1: KafkaServer = null
  var server2: KafkaServer = null
  var server3: KafkaServer = null
  var metadataStore: TopicMetadataStore = null

  @BeforeClass
  def beforeSetupServers {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    server1 = TestUtils.createServer(config1)
    server2 = TestUtils.createServer(config2)
    server3 = TestUtils.createServer(config3)
    zkClient = new ZkClient(zkConnect + "/", 6000, 6000, ZKStringSerializer)
    producer = new Producer(producerConfig)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")
  }

  def createTopic {
    AdminUtils.createTopic(
      zkClient,
      TOPIC,
      TOTAL_PARTITIONS,
      REPLICATION_FACTOR)
  }

  def validateTopic {
    var done = false
    var retries = 0
    val maxRetries = 100

    while (!done && retries < maxRetries) {
      try {
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(TOPIC), "kafka", metadataStore.getTopicInfo)
        val topicMetadata = topicMetadataMap(TOPIC)
        val errorCode = topicMetadata.errorCode

        ErrorMapping.maybeThrowException(errorCode)

        done = true
      } catch {
        case e: Throwable =>
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

class TestKafkaSystemAdmin {
  import TestKafkaSystemAdmin._

  @Test
  def testShouldGetLastOffsets {
    val systemName = "test"
    val systemAdmin = new KafkaSystemAdmin(systemName, brokers)

    // Get a non-existent topic.
    val initialInputOffsets = systemAdmin.getLastOffsets(Set("foo"))
    assertEquals(0, initialInputOffsets.size)

    // Create an empty topic with 50 partitions, but with no offsets.
    createTopic
    validateTopic
    val createdInputOffsets = systemAdmin.getLastOffsets(Set(TOPIC))
    assertEquals(0, createdInputOffsets.size)

    // Add a new message to one of the partitions.
    producer.send(new KeyedMessage(TOPIC, "key1", "val1"))
    val oneMessageInputOffsets = systemAdmin.getLastOffsets(Set(TOPIC))
    val ssp = oneMessageInputOffsets.keySet.head
    assertEquals(1, oneMessageInputOffsets.size)
    assertEquals(systemName, ssp.getSystem)
    assertEquals(TOPIC, ssp.getStream)
    // key1 gets hash-mod'd to partition 48.
    assertEquals(48, ssp.getPartition.getPartitionId)

    // Validate that a fetch will return the message.
    val connector = getConsumerConnector
    var stream = connector.createMessageStreams(Map(TOPIC -> 1)).get(TOPIC).get.get(0).iterator
    val message = stream.next
    val text = new String(message.message, "UTF-8")
    connector.shutdown
    // Message's offset should match the expected latest offset.
    assertEquals(oneMessageInputOffsets(ssp), message.offset.toString)
    assertEquals("val1", text)
  }
}