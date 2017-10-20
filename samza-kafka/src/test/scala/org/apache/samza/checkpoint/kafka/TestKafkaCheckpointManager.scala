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


import java.util.Properties

import _root_.kafka.admin.AdminUtils
import _root_.kafka.common.{InvalidMessageSizeException, UnknownTopicOrPartitionException}
import _root_.kafka.integration.KafkaServerTestHarness
import _root_.kafka.message.InvalidMessageException
import _root_.kafka.server.{ConfigType, KafkaConfig}
import _root_.kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.security.JaasUtils
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system._
import org.apache.samza.system.kafka.{KafkaSystemAdmin, KafkaStreamSpec}
import org.apache.samza.util.{ClientUtilTopicMetadataStore, KafkaUtilException, NoOpMetricsRegistry, TopicMetadataStore}
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConverters._
import scala.collection._

class TestKafkaCheckpointManager extends KafkaServerTestHarness {

  protected def numBrokers: Int = 3

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, true)
    props.map(KafkaConfig.fromProps)
  }

  val checkpointTopic = "checkpoint-topic"
  val serdeCheckpointTopic = "checkpoint-topic-invalid-serde"
  val checkpointTopicConfig = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(null)
  val checkpointKeySerde = new KafkaCheckpointLogKeySerde

  val zkSecure = JaasUtils.isZkSecurityEnabled()

  val partition = new Partition(0)
  val partition2 = new Partition(1)
  val cp1 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "123").asJava)
  val cp2 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "12345").asJava)

  var producerConfig: KafkaProducerConfig = null

  var metadataStore: TopicMetadataStore = null
  var failOnTopicValidation = true

  val systemStreamPartitionGrouperFactoryString = classOf[GroupByPartitionFactory].getCanonicalName

  val systemName = "kafka"
  val CHECKPOINT_STREAMID = "unused-temp-checkpoint-stream-id"
  val kafkaStreamSpec = new KafkaStreamSpec(CHECKPOINT_STREAMID,
                                 checkpointTopic, systemName, 1,
                                 1, new Properties())

  var systemFactory: SystemFactory = _
  var systemProducer: SystemProducer = _
  var systemConsumer: SystemConsumer = _
  var systemAdmin: SystemAdmin = _
  var config: MapConfig = _
  @Before
  override def setUp {
    super.setUp

    TestUtils.waitUntilTrue(() => servers.head.metadataCache.getAliveBrokers.size == numBrokers, "Wait for cache to update")

    val brokers = brokerList.split(",").map(p => "localhost" + p).mkString(",")
    val configMap = new java.util.HashMap[String, String]()
    configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    configMap.put("acks", "all")
    configMap.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    configMap.put(ProducerConfig.RETRIES_CONFIG, (new Integer(java.lang.Integer.MAX_VALUE-1)).toString)
    configMap.putAll(KafkaCheckpointManagerFactory.INJECTED_PRODUCER_PROPERTIES.asJava)
    producerConfig = new KafkaProducerConfig(systemName, "i001", configMap)

    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")

    configMap.put(SystemConfig.SYSTEM_FACTORY format systemName, "org.apache.samza.system.kafka.KafkaSystemFactory")
    configMap.put(org.apache.samza.config.KafkaConfig.CHECKPOINT_SYSTEM, systemName);
    configMap.put(JobConfig.JOB_NAME, "some-job-name");
    configMap.put(JobConfig.JOB_ID, "i001");
    configMap.put("systems.%s.producer.%s" format (systemName, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), brokers)
    configMap.put("systems.%s.consumer.zookeeper.connect" format systemName, zkConnect)
    val cfg: SystemConfig = new SystemConfig(new MapConfig(configMap))
    val (checkpointSystem: String, factory : SystemFactory) =
      KafkaCheckpointManagerFactory.getCheckpointSystemStreamAndFactory(cfg)

    config = new MapConfig(configMap)
    systemProducer = factory.getProducer(checkpointSystem, config , new NoOpMetricsRegistry)
    systemConsumer = factory.getConsumer(checkpointSystem, config, new NoOpMetricsRegistry)
    systemAdmin = factory.getAdmin(systemName, config)
    systemFactory = factory
  }

  @After
  override def tearDown() {
    if (servers != null) {
      servers.foreach(_.shutdown())
      servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    }
    super.tearDown
  }

  private def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint, cpTopic: String = checkpointTopic) = {
    val producer: Producer[Array[Byte], Array[Byte]] = new KafkaProducer(producerConfig.getProducerProperties)
    val record = new ProducerRecord(
      cpTopic,
      0,
      checkpointKeySerde.toBytes(new KafkaCheckpointLogKey(systemStreamPartitionGrouperFactoryString, taskName, KafkaCheckpointLogKey.CHECKPOINT_TYPE)),
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


  private def createCheckpointTopic(cpTopic: String = checkpointTopic, partNum: Int = 1) = {
    val zkClient = ZkUtils(zkConnect, 6000, 6000, zkSecure)
    try {
      AdminUtils.createTopic(
        zkClient,
        cpTopic,
        partNum,
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
    systemAdmin.validateStream(kafkaStreamSpec)

    // check that log compaction is enabled.
    val zkClient = ZkUtils(zkConnect, 6000, 6000, zkSecure)
    val topicConfig = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic, checkpointTopic)
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
  def testCheckpointReadTwice {
    val kcm = getKafkaCheckpointManager
    val taskName = new TaskName(partition.toString)
    kcm.register(taskName)
    createCheckpointTopic()
    systemAdmin.validateStream(kafkaStreamSpec)


    // check that log compaction is enabled.
    val zkClient = ZkUtils(zkConnect, 6000, 6000, zkSecure)
    val topicConfig = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic, checkpointTopic)
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

    // writing a second message should work, too
    writeCheckpoint(taskName, cp2)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(cp2, readCp)
    kcm.stop

    // get new KCM for the same stream
    val kcm1 = getKafkaCheckpointManager
    kcm1.register(taskName)
    readCp = kcm1.readLastCheckpoint(taskName)
    assertEquals(cp2, readCp)
    kcm1.stop
  }

  @Test
  def testUnrecoverableKafkaErrorShouldThrowKafkaCheckpointManagerException {
    val exceptions = List("InvalidMessageException", "InvalidMessageSizeException", "UnknownTopicOrPartitionException")
    exceptions.foreach { exceptionName =>
      val kcm = getKafkaCheckpointManagerWithInvalidSerde(exceptionName)
      val taskName = new TaskName(partition.toString)
      kcm.register(taskName)
      createCheckpointTopic(serdeCheckpointTopic)
      systemAdmin.validateStream(kafkaStreamSpec)

      writeCheckpoint(taskName, cp1, serdeCheckpointTopic)
      // because serde will throw unrecoverable errors, it should result a KafkaCheckpointException
      try {
        kcm.readLastCheckpoint(taskName)
        fail("Expected an Exception.")
      } catch {
        case e: KafkaUtilException => None
        case e: Exception => None
      }
      kcm.stop
    }
  }

  @Test
  def testFailOnTopicValidation {
    // first case - default case, we should fail on validation
    failOnTopicValidation = true
    val checkpointTopic8 = checkpointTopic + "8";
    val kcm = getKafkaCheckpointManagerWithParam(checkpointTopic8)
    val taskName = new TaskName(partition.toString)
    kcm.register(taskName)
    createCheckpointTopic(checkpointTopic8, 8) // create topic with the wrong number of partitions
    try {
      kcm.start
      fail("Expected a KafkaUtilException for invalid number of partitions in the topic.")
    }catch {
      case e: StreamValidationException => None
    }
    kcm.stop

    // same validation but ignore the validation error (pass 'false' to validate..)
    failOnTopicValidation = false
    val kcm1 = getKafkaCheckpointManagerWithParam((checkpointTopic8))
    kcm1.register(taskName)
    try {
      kcm1.start
    }catch {
      case e: KafkaUtilException => fail("Did not expect a KafkaUtilException for invalid number of partitions in the topic.")
    }
    kcm1.stop
  }

  private def getKafkaCheckpointManagerWithParam(cpTopic: String) = {
    val spec = new KafkaStreamSpec("id", cpTopic, systemName, 1, 1, KafkaCheckpointManagerFactory.getCheckpointTopicProperties(new MapConfig()))
    new KafkaCheckpointManager(spec, systemFactory, systemStreamPartitionGrouperFactoryString, true, config, new NoOpMetricsRegistry)
  }

  // CheckpointManager with a specific checkpoint topic
  private def getKafkaCheckpointManager = getKafkaCheckpointManagerWithParam(checkpointTopic)

  // inject serde. Kafka exceptions will be thrown when serde.fromBytes is called
  private def getKafkaCheckpointManagerWithInvalidSerde(exception: String) = {
    val spec = new KafkaStreamSpec("id", serdeCheckpointTopic, systemName, 1, 1, KafkaCheckpointManagerFactory.getCheckpointTopicProperties(new MapConfig()))
    new KafkaCheckpointManager(spec, systemFactory, systemStreamPartitionGrouperFactoryString, true, config, new NoOpMetricsRegistry)
  }

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
