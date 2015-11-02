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

import kafka.admin.AdminUtils
import kafka.common.{InvalidMessageSizeException, UnknownTopicOrPartitionException}
import kafka.message.InvalidMessageException
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, TestZKUtils, Utils, ZKStringSerializer}
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.config.{KafkaProducerConfig, MapConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.{ClientUtilTopicMetadataStore, KafkaUtilException, TopicMetadataStore}
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConversions._
import scala.collection._

class TestKafkaCheckpointManager {

  val checkpointTopic = "checkpoint-topic"
  val serdeCheckpointTopic = "checkpoint-topic-invalid-serde"
  val checkpointTopicConfig = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(null)
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

  val config = new java.util.HashMap[String, Object]()
  val brokers = "localhost:%d,localhost:%d,localhost:%d" format (port1, port2, port3)
  config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  config.put("acks", "all")
  config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
  config.put(ProducerConfig.RETRIES_CONFIG, (new Integer(java.lang.Integer.MAX_VALUE-1)).toString)
  config.putAll(KafkaCheckpointManagerFactory.INJECTED_PRODUCER_PROPERTIES)
  val producerConfig = new KafkaProducerConfig("kafka", "i001", config)
  val partition = new Partition(0)
  val partition2 = new Partition(1)
  val cp1 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "123"))
  val cp2 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "12345"))
  var zookeeper: EmbeddedZookeeper = null
  var server1: KafkaServer = null
  var server2: KafkaServer = null
  var server3: KafkaServer = null
  var metadataStore: TopicMetadataStore = null

  val systemStreamPartitionGrouperFactoryString = classOf[GroupByPartitionFactory].getCanonicalName

  @Before
  def beforeSetupServers {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    server1 = TestUtils.createServer(new KafkaConfig(props1))
    server2 = TestUtils.createServer(new KafkaConfig(props2))
    server3 = TestUtils.createServer(new KafkaConfig(props3))
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")
  }

  @After
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

  private def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint, cpTopic: String = checkpointTopic) = {
    val producer: Producer[Array[Byte], Array[Byte]] = new KafkaProducer(producerConfig.getProducerProperties)
    val record = new ProducerRecord(
      cpTopic,
      0,
      KafkaCheckpointLogKey.getCheckpointKey(taskName).toBytes(),
      new CheckpointSerde().toBytes(checkpoint)
    )
    try {
      producer.send(record).get()
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      producer.close()
    }
  }


  private def createCheckpointTopic(cpTopic: String = checkpointTopic) = {
    val zkClient = new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer)
    try {
      AdminUtils.createTopic(
        zkClient,
        checkpointTopic,
        1,
        1,
        checkpointTopicConfig)
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      zkClient.close
    }
  }

  @Test
  def testCheckpointShouldBeNullIfCheckpointTopicDoesNotExistShouldBeCreatedOnWriteAndShouldBeReadableAfterWrite {
    val kcm = getKafkaCheckpointManager
    val taskName = new TaskName(partition.toString)
    kcm.register(taskName)
    createCheckpointTopic()
    kcm.kafkaUtil.validateTopicPartitionCount(checkpointTopic, "kafka", metadataStore, 1)

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
    writeCheckpoint(taskName, cp1)
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
    writeCheckpoint(taskName, cp2)
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
      createCheckpointTopic(serdeCheckpointTopic)
      kcm.kafkaUtil.validateTopicPartitionCount(serdeCheckpointTopic, "kafka", metadataStore, 1)
      writeCheckpoint(taskName, cp1, serdeCheckpointTopic)
      // because serde will throw unrecoverable errors, it should result a KafkaCheckpointException
      try {
        kcm.readLastCheckpoint(taskName)
        fail("Expected a KafkaUtilException.")
      } catch {
        case e: KafkaUtilException => None
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
    connectProducer = () => new KafkaProducer(producerConfig.getProducerProperties),
    connectZk = () => new ZkClient(zkConnect, 60000, 60000, ZKStringSerializer),
    systemStreamPartitionGrouperFactoryString = systemStreamPartitionGrouperFactoryString,
    checkpointTopicProperties = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(new MapConfig(Map[String, String]())))

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
    connectProducer = () => new KafkaProducer(producerConfig.getProducerProperties),
    connectZk = () => new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer),
    systemStreamPartitionGrouperFactoryString = systemStreamPartitionGrouperFactoryString,
    serde = new InvalideSerde(exception),
    checkpointTopicProperties = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(new MapConfig(Map[String, String]())))

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
