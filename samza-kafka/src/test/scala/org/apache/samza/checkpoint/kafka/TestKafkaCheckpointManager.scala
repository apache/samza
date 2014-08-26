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

package org.apache.samza.checkpoint.kafka

import kafka.common.InvalidMessageSizeException
import kafka.common.UnknownTopicOrPartitionException
import kafka.message.InvalidMessageException
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.utils.TestZKUtils
import kafka.utils.Utils
import kafka.utils.ZKStringSerializer
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.container.TaskName
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.{ ClientUtilTopicMetadataStore, TopicMetadataStore }
import org.apache.samza.{ SamzaException, Partition }
import org.junit.Assert._
import org.junit.{ AfterClass, BeforeClass, Test }
import scala.collection.JavaConversions._
import scala.collection._
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import kafka.admin.AdminUtils

object TestKafkaCheckpointManager {
  val checkpointTopic = "checkpoint-topic"
  val serdeCheckpointTopic = "checkpoint-topic-invalid-serde"
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
  props1.put("controlled.shutdown.enable", "true")
  val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
  props1.put("controlled.shutdown.enable", "true")
  val props3 = TestUtils.createBrokerConfig(brokerId3, port3)
  props1.put("controlled.shutdown.enable", "true")

  val config = new java.util.Properties()
  val brokers = "localhost:%d,localhost:%d,localhost:%d" format (port1, port2, port3)
  config.put("metadata.broker.list", brokers)
  config.put("producer.type", "sync")
  config.put("request.required.acks", "-1")
  config.putAll(KafkaCheckpointManagerFactory.INJECTED_PRODUCER_PROPERTIES)
  val producerConfig = new ProducerConfig(config)
  val partition = new Partition(0)
  val cp1 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "123"))
  val cp2 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "12345"))
  var zookeeper: EmbeddedZookeeper = null
  var server1: KafkaServer = null
  var server2: KafkaServer = null
  var server3: KafkaServer = null
  var metadataStore: TopicMetadataStore = null

  val systemStreamPartitionGrouperFactoryString = classOf[GroupByPartitionFactory].getCanonicalName

  @BeforeClass
  def beforeSetupServers {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    server1 = TestUtils.createServer(new KafkaConfig(props1))
    server2 = TestUtils.createServer(new KafkaConfig(props2))
    server3 = TestUtils.createServer(new KafkaConfig(props3))
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")
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
    zookeeper.shutdown
  }
}

class TestKafkaCheckpointManager {
  import TestKafkaCheckpointManager._

  @Test
  def testCheckpointShouldBeNullIfCheckpointTopicDoesNotExistShouldBeCreatedOnWriteAndShouldBeReadableAfterWrite {
    val kcm = getKafkaCheckpointManager
    val taskName = new TaskName(partition.toString)
    kcm.register(taskName)
    kcm.start
    // check that log compaction is enabled.
    val zkClient = new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer)
    val topicConfig = AdminUtils.fetchTopicConfig(zkClient, checkpointTopic)
    zkClient.close
    assertEquals("compact", topicConfig.get("cleanup.policy"))
    assertEquals("26214400", topicConfig.get("segment.bytes"))
    // read before topic exists should result in a null checkpoint
    var readCp = kcm.readLastCheckpoint(taskName)
    assertNull(readCp)
    // create topic the first time around
    kcm.writeCheckpoint(taskName, cp1)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(cp1, readCp)
    // should get an exception if partition doesn't exist
    try {
      readCp = kcm.readLastCheckpoint(new TaskName(new Partition(1).toString))
      fail("Expected a SamzaException, since only one partition (partition 0) should exist.")
    } catch {
      case e: SamzaException => None // expected
      case _: Exception => fail("Expected a SamzaException, since only one partition (partition 0) should exist.")
    }
    // writing a second message should work, too
    kcm.writeCheckpoint(taskName, cp2)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(cp2, readCp)
    kcm.stop
  }

  @Test
  def testUnrecoverableKafkaErrorShouldThrowKafkaCheckpointManagerException {
    val exceptions = List("InvalidMessageException", "InvalidMessageSizeException", "UnknownTopicOrPartitionException")
    exceptions.foreach { exceptionName =>
      val kcm = getKafkaCheckpointManagerWithInvalidSerde(exceptionName)
      val taskName = new TaskName(partition.toString)
      kcm.register(taskName)
      kcm.start
      kcm.writeCheckpoint(taskName, cp1)
      // because serde will throw unrecoverable errors, it should result a KafkaCheckpointException
      try {
        kcm.readLastCheckpoint(taskName)
        fail("Expected a KafkaCheckpointException.")
      } catch {
        case e: KafkaCheckpointException => None
      }
      kcm.stop
    }
  }

  private def getKafkaCheckpointManager = new KafkaCheckpointManager(
    clientId = "some-client-id",
    checkpointTopic = checkpointTopic,
    systemName = "kafka",
    replicationFactor = 3,
    socketTimeout = 30000,
    bufferSize = 64 * 1024,
    fetchSize = 300 * 1024,
    metadataStore = metadataStore,
    connectProducer = () => new Producer[Array[Byte], Array[Byte]](producerConfig),
    connectZk = () => new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer),
    systemStreamPartitionGrouperFactoryString = systemStreamPartitionGrouperFactoryString,
    checkpointTopicProperties = KafkaCheckpointManagerFactory.CHECKPOINT_TOPIC_PROPERTIES)

  // inject serde. Kafka exceptions will be thrown when serde.fromBytes is called
  private def getKafkaCheckpointManagerWithInvalidSerde(exception: String) = new KafkaCheckpointManager(
    clientId = "some-client-id-invalid-serde",
    checkpointTopic = serdeCheckpointTopic,
    systemName = "kafka",
    replicationFactor = 3,
    socketTimeout = 30000,
    bufferSize = 64 * 1024,
    fetchSize = 300 * 1024,
    metadataStore = metadataStore,
    connectProducer = () => new Producer[Array[Byte], Array[Byte]](producerConfig),
    connectZk = () => new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer),
    systemStreamPartitionGrouperFactoryString = systemStreamPartitionGrouperFactoryString,
    serde = new InvalideSerde(exception),
    checkpointTopicProperties = KafkaCheckpointManagerFactory.CHECKPOINT_TOPIC_PROPERTIES)

  class InvalideSerde(exception: String) extends CheckpointSerde {
    override def fromBytes(bytes: Array[Byte]): Checkpoint = {
      exception match {
        case "InvalidMessageException" => throw new InvalidMessageException
        case "InvalidMessageSizeException" => throw new InvalidMessageSizeException
        case "UnknownTopicOrPartitionException" => throw new UnknownTopicOrPartitionException
      }
    }
  }
}
