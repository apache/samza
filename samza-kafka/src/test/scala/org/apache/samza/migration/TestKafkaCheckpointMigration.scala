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

package org.apache.samza.migration

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, TestZKUtils, Utils}
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.checkpoint.kafka.{KafkaCheckpointManager, KafkaCheckpointLogKey, KafkaCheckpointManagerFactory}
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.coordinator.MockSystemFactory
import org.apache.samza.coordinator.stream.messages.SetMigrationMetaMessage
import org.apache.samza.coordinator.stream._
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.storage.ChangelogPartitionManager
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util._
import org.apache.samza.Partition
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConversions._
import scala.collection._

class TestKafkaCheckpointMigration {

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

  private def writeChangeLogPartitionMapping(changelogMapping: Map[TaskName, Integer], cpTopic: String = checkpointTopic) = {
    val producer: Producer[Array[Byte], Array[Byte]] = new KafkaProducer(producerConfig.getProducerProperties)
    val record = new ProducerRecord(
      cpTopic,
      0,
      KafkaCheckpointLogKey.getChangelogPartitionMappingKey().toBytes(),
      new CheckpointSerde().changelogPartitionMappingToBytes(changelogMapping)
    )
    try {
      producer.send(record).get()
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      producer.close()
    }
  }

  @Test
  def testMigrationWithNoCheckpointTopic() {
    val mapConfig = Map[String, String](
      "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
      JobConfig.JOB_NAME -> "test",
      JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
      JobConfig.JOB_CONTAINER_COUNT -> "2",
      "task.inputs" -> "test.stream1",
      "task.checkpoint.system" -> "test",
      SystemConfig.SYSTEM_FACTORY.format("test") -> classOf[MockSystemFactory].getCanonicalName,
      "systems.test.producer.bootstrap.servers" -> brokers,
      "systems.test.consumer.zookeeper.connect" -> zkConnect,
      SystemConfig.SYSTEM_FACTORY.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName)

    // Enable consumer caching
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache()

    val config: MapConfig = new MapConfig(mapConfig)
    val migrate = new KafkaCheckpointMigration
    migrate.migrate(config)
    val consumer = new CoordinatorStreamSystemFactory().getCoordinatorStreamSystemConsumer(config, new NoOpMetricsRegistry)
    consumer.register()
    consumer.start()
    consumer.bootstrap()
    val bootstrappedStream = consumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE)
    assertEquals(1, bootstrappedStream.size())

    val expectedMigrationMessage = new SetMigrationMetaMessage("CHECKPOINTMIGRATION", "CheckpointMigration09to10", "true")
    assertEquals(expectedMigrationMessage, bootstrappedStream.head)
    consumer.stop()
  }

  @Test
  def testMigration() {
    try {
      val mapConfig = Map(
        "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
        JobConfig.JOB_NAME -> "test",
        JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
        JobConfig.JOB_CONTAINER_COUNT -> "2",
        "task.inputs" -> "test.stream1",
        "task.checkpoint.system" -> "test",
        "systems.test.producer.bootstrap.servers" -> brokers,
        "systems.test.consumer.zookeeper.connect" -> zkConnect,
        SystemConfig.SYSTEM_FACTORY.format("test") -> classOf[MockSystemFactory].getCanonicalName,
        SystemConfig.SYSTEM_FACTORY.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName)
      // Enable consumer caching
      MockCoordinatorStreamSystemFactory.enableMockConsumerCache()

      val config = new MapConfig(mapConfig)
      val checkpointTopicName = KafkaUtil.getCheckpointTopic("test", "1")
      val checkpointManager = new KafkaCheckpointManagerFactory().getCheckpointManager(config, new NoOpMetricsRegistry).asInstanceOf[KafkaCheckpointManager]

      // Write a couple of checkpoints in the old checkpoint topic
      val task1 = new TaskName(partition.toString)
      val task2 = new TaskName(partition2.toString)
      checkpointManager.start
      checkpointManager.register(task1)
      checkpointManager.register(task2)
      checkpointManager.writeCheckpoint(task1, cp1)
      checkpointManager.writeCheckpoint(task2, cp2)

      // Write changelog partition info to the old checkpoint topic
      val changelogMapping = Map(task1 -> 1.asInstanceOf[Integer], task2 -> 10.asInstanceOf[Integer])
      writeChangeLogPartitionMapping(changelogMapping, checkpointTopicName)
      checkpointManager.stop

      // Initialize coordinator stream
      val coordinatorFactory = new CoordinatorStreamSystemFactory()
      val coordinatorSystemConsumer = coordinatorFactory.getCoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
      val coordinatorSystemProducer = coordinatorFactory.getCoordinatorStreamSystemProducer(config, new MetricsRegistryMap)
      coordinatorSystemConsumer.register()
      coordinatorSystemConsumer.start()

      assertEquals(coordinatorSystemConsumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE).size, 0)
      coordinatorSystemConsumer.stop

      // Start the migration
      val migrationInstance = new KafkaCheckpointMigration
      migrationInstance.migrate(config)

      // Verify if the changelogPartitionInfo has been migrated
      val newChangelogManager = new ChangelogPartitionManager(coordinatorSystemProducer, coordinatorSystemConsumer, "test")
      newChangelogManager.start
      val newChangelogMapping = newChangelogManager.readChangeLogPartitionMapping()
      newChangelogManager.stop
      assertEquals(newChangelogMapping.toMap, changelogMapping)

      // Check for migration message
      coordinatorSystemConsumer.register()
      coordinatorSystemConsumer.start()
      assertEquals(coordinatorSystemConsumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE).size, 1)
      coordinatorSystemConsumer.stop()
    }
    finally {
      MockCoordinatorStreamSystemFactory.disableMockConsumerCache()
    }
  }

  class MockKafkaCheckpointMigration extends KafkaCheckpointMigration{
    var migrationCompletionMarkFlag: Boolean = false
    var migrationVerificationMarkFlag: Boolean = false

    override def migrationCompletionMark(coordinatorStreamProducer: CoordinatorStreamSystemProducer) = {
      migrationCompletionMarkFlag = true
      super.migrationCompletionMark(coordinatorStreamProducer)
    }

    override def migrationVerification(coordinatorStreamConsumer: CoordinatorStreamSystemConsumer): Boolean = {
      migrationVerificationMarkFlag = true
      super.migrationVerification(coordinatorStreamConsumer)
    }
  }
}
